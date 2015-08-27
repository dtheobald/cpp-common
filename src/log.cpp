/**
 * @file log.cpp
 *
 * Project Clearwater - IMS in the Cloud
 * Copyright (C) 2013  Metaswitch Networks Ltd
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version, along with the "Special Exception" for use of
 * the program along with SSL, set forth below. This program is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details. You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * The author can be reached by email at clearwater@metaswitch.com or by
 * post at Metaswitch Networks Ltd, 100 Church St, Enfield EN2 6BQ, UK
 *
 * Special Exception
 * Metaswitch Networks Ltd  grants you permission to copy, modify,
 * propagate, and distribute a work formed by combining OpenSSL with The
 * Software, or a work derivative of such a combination, even if such
 * copying, modification, propagation, or distribution would otherwise
 * violate the terms of the GPL. You must comply with the GPL in all
 * respects for all of the code used other than OpenSSL.
 * "OpenSSL" means OpenSSL toolkit software distributed by the OpenSSL
 * Project and licensed under the OpenSSL Licenses, or a work based on such
 * software and licensed under the OpenSSL Licenses.
 * "OpenSSL Licenses" means the OpenSSL License and Original SSLeay License
 * under which the OpenSSL Project distributes the OpenSSL toolkit software,
 * as those licenses appear in the file LICENSE-OPENSSL.
 */


#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <algorithm>
#include "log.h"

const char* log_level[] = {"Error", "Warning", "Status", "Info", "Verbose", "Debug"};

#define MAX_LOGLINE 8192
#define RAM_BUFSIZE (1024 * 1024)

// This macro gives the size of a trace entry in the RAM buffer for a given
// number of parameters.
//
// In principle, we should worry about whether adjacent trace entries are
// going to be aligned properly - in the worst case, we might get an alignment
// exception if we attempt to point at a trace entry in the buffer that
// starts, e.g., on an odd address.  Fortunately, the fact that the last entry in the
// structure is the one with the greatest alignment restrictions (containing
// the largest possible C basic types) there is no risk that the size of the
// record is going to give us alignment problems of this type.
#define RAM_SIZE(NUM_PARAMS) ((offsetof(TRC_RAMBUF_ENTRY, params)) + (NUM_PARAMS * TRC_RAMBUF_PARAM))
#define RAM_ENTRY_SIZE(ENTRY) RAM_SIZE(Log::ram_trc_cache[ENTRY->trc_id - 1]->num_params)

// The following structure is used to cache the parameters expected on a given
// TRC_... call in the code.  Each entry is associated with a specific trace
// ID which is written into each trace entry in the RAM buffer so that the
// format string, module and line number don't have to be reparsed and
// rewritten into the RAM buffer for every trace call.
//
// The trace id is 1-based so that
// -  Zero filling unused space at the end of the RAM buffer following a wrap
//    means that such space can be easily identified as not containing a
//    a valid trace entry
// -  The calling code (see TRC_RAMTRACE in log.h) can safely treat a trc_id
//    value of zero as meaning "trace call not cached yet".
typedef struct
{
  int         line_number;
  const char *module;
  const char *fmt;
  int         num_params;
  int         param_types[1];
} TRC_RAMTRC_CACHE;

// The following structure is a union of basic types that can be used to
// store an basic type that can be passed to a printf call.  It is used by the
// RAM trace code to store the parameter data passed on a given trace instance.
// The decoder uses the trace cache to determine how to interpret each value.
typedef union
{
  char c;
  int i;
  short s;
  long l;
  long long ll;
  float f;
  double d;
  long double ld;
  void *p;
} TRC_RAMBUF_PARAM;

// The following structure defines an entry in the cycle RAM trace buffer
//
// @@@ Note that this implementation is a bit wasteful on space, as each entry
// in params is 128 bits long (the size of a long long or long double),
// despite the fact that the vast majority of parameters are going to be 32-bit
// ints.  Packing parameter data on byte boundaries would be an alternative
// implementation that would be much more efficient in its use of space.
// However, in order to avoid alignment issues, parameter values would need to
// be memcpied into place which is far less performance-efficient for ints
// than straightforward assignment. @@@
typedef struct
{
  pthread_t        thread;
  int              trc_id;
  TRC_RAMBUF_PARAM params[1];

} TRC_RAMBUF_ENTRY;

namespace Log
{
  static Logger logger_static;
  static Logger *logger = &logger_static;
  static pthread_mutex_t serialization_lock = PTHREAD_MUTEX_INITIALIZER;
  int loggingLevel = 4;

  // RAM buffer and associated static parameters.  The RAM buffer consists of
  // an area of memory (of arbitrary size) to which structures of type
  // TRC_RAMBUF_ENTRY are written (these structures being of variable size
  // depending on the number of arguments represented by the entry).  Note
  // that the buffer is deliberately unioned with a TRC_RAMBUF_ENTRY to ensure
  // that it is correctly aligned.
  //
  // ram_next_slot points to the next available slot in the buffer (which might
  // or might not be big enough for the next record to be written - if not, the
  // buffer is wrapped when that next record is written).
  //
  // ram_head_slot points to the oldest TRC_RAMUF_ENTRY structure in the buffer.
  //
  // Special care needs to be taken when wrapping (i.e. when the next record to
  // be written won't fit in the buffer).  We need to make sure that any
  // existing valid log data at the end of the buffer that we're having to skip
  // over gets blatted to zero so that the dump tool doesn't regard the
  // skipped bit as containing valid log data and spuriously dump rubbish.  As
  // module cannot be validly NULL, just checking whether this field is NULL
  // is enough to tell the dumper that its reached the end of the buffer
  // and needs to wrap back to the start.
  static union
  {
    char buf[RAM_BUFSIZE];
    TRC_RAMBUF_ENTRY align;
  } ram_buffer = 0;
  static pthread_mutex_t ram_lock = PTHREAD_MUTEX_INITIALIZER;
  static TRC_RAMBUF_ENTRY* ram_next_slot = &ram_buffer.align;
  static TRC_RAMBUF_ENTRY* ram_head_slot = NULL;

  // The trace cache is an inventory of all the trace calls in the code,
  // built up dynamically as trace calls are made.  It avoids the need to
  // duplicate the details of a given log call in the trace buffer or parse
  // the format string every time that trace call is made.
  //
  // It is indexed by ("trace id" - 1).
  static TRC_RAMTRC_CACHE** ram_trc_cache = NULL;
  static int ram_trc_cache_len = 0;
}

// Fast RAM buffer write routine
void LOG::ramTrace(int trc_id, char *fmt, ...)
{
  va_list args;
  va_start(args, fmt);

  // Lock access to the RAM buffer

  pthread_mutex_lock(&Log::ram_lock);

  int entry_size = RAM_SIZE(num_params);

  // Find space in the buffer to write our trace entry into
  if (((char *)Log::ram_next_slot + entry_size) > (Log::ram_buffer.buf + RAM_BUFSIZE))
  {
    // The trace isn't going to fit at the end of the buffer, so we need to wrap it.
    // Set any remaining space in the buffer to zero (so that it can't be accidentally
    // interpreted as good data).
    memset((char *)Log::ram_next_slot, 0, RAM_BUFSIZE - ((char *)Log::ram_next_slot - Log::ram_buffer.buf));

    // Check whether the head slot is in the space we have just destroyed (in which case this
    // needs to be reset to the start of the buffer).
    if (Log::ram_head_slot >= Log::ram_next_slot)
    {
      Log::ram_head_slot = &Log::ram_buffer.align;
    }

    // Point the next slot at the head of the buffer
    Log::ram_next_slot = &Log::ram_buffer.align;
  }

  // If we have an oldest trace in the buffer (i.e., a "head" entry), bump this
  // along if we're about to overwrite it
  while ((Log::ram_head_slot != NULL) && ((char *)Log::ram_next_slot + entry_size) > RAM_ENTRY_SIZE(Log::ram_head_slot))
  {
    Log::ram_head_slot = (char *)Log::ram_head_slot + RAM_ENTRY_SIZE(Log::ram_head_slot);

    if ((((char *)Log::ram_head_slot + RAM_SIZE(0)) > (Log::ram_buffer.buf + RAM_BUFSIZE)) ||
        (Log::ram_head_slot->trc_id == 0))
    {
      // Head slot no longer points to a valid thread entry.  Wrap it and stop looking
      Log::ram_head_slot = &Log::ram_buffer.align;
      break;
    }
  }

  // We now have a slot we can write our trace entry into and a head entry
  // which we are not about to overwrite.

  // Save off a pointer to the slot and bump the next_slot pointer past it.
  // Note that it doesn't matter if there isn't enough space in the buffer to
  // write here - the next tracing thread will spot this and wrap it
  // automatically.
  TRC_RAMBUF_ENTRY *this_slot = Log::ram_next_slot;
  Log::ram_next_slot = (char *)Log::ram_next_slot + entry_size;

  // If we haven't got a "head" entry yet, this is it
  if (Log::ram_head_slot == NULL)
  {
    Log::ram_head_slot = this_slot;
  }

  // Fill in the fields in the slot
  this_slot->thread = pthread_self();
  this_slot->trc_id = trc_id;

  // Fill in the parameters, if any.  This is slightly involved, as we need to
  // be careful to pull the arguments off the stack according to their size,
  // which we determine from the trace cache entry.
  TRC_RAMTRC_CACHE* this_cache = Log::ram_trc_cache[trc_id - 1];
  int *type = this_cache->param_types;
  TRC_RAMBUF_PARAM *value = this_slot->params;

  for (int i = 0; i < this_cache->num_params; i++)
  {
    if (*type & PA_FLAG_PTR)
    {
      // This is a pointer and can be safely treated as a void *
      value->p = va_arg(args, void *);
    }
    else
    {
      switch (*type & ~PA_FLAG_MASK)
      {
      case PA_INT:
        // use int, short, long or long long as appropriate.  Check for the
        // int case first for performance thats the most likely type.

        if (!(*type & (PA_FLAG_SHORT|PA_FLAG_LONG|PA_FLAG_LONG_LONG)))
        {
          value->i = va_arg(args, int);
        }
        else if (*type & PA_FLAG_SHORT)
        {
          value->s = va_arg(args, short);
        }
        else if (*type & PA_FLAG_LONG)
        {
          value->l = va_arg(args, long);
        }
        else
        {
          // Must be a long long
          value->ll = va_arg(args, long long);
        }
        break;

      case PA_CHAR:
        value->c = va_arg(args, char);
        break;

      case PA_WCHAR:
        value->w = va_arg(args, wide char);
        break;

      case PA_STRING:
      case PA_WSTRING:
      case PA_POINTER:
        // These can all be treated as void *s
        value->p = va_arg(args, void *);
        break;

      case PA_FLOAT:
        value->f = va_arg(args, float);
        break;

      case PA_DOUBLE:
        // Allow for a long double
        if (*type & PA_FLAG_LONG_DOUBLE)
        {
          value->ld = va_arg(args, long double);
        }
        else
        {
          value->d = va_arg(args, double);
        }
        break;

      default:
        // Assume any other parameter is an int
        value->i = va_arg(args, int);
        break;
      }
    }

    // Next type/value
    type++;
    value++;
  }

  // Release the ram buffer lock
  pthread_mutex_unlock(&Log::ram_lock);

  va_end(args);
}


// RAM buffer decode routine
//
// Note that this routine does not acquire locks.  It is intended to be run
// via gdb against a core file and is hardened to allow for possible data
// corruption
//
// If it is ever decided to run this live, expose a separate ram_lock wrapper
// for this routine, but don't do call such a wrapper from a termination
// signal handler or you may deadlock against a thread which has been
// terminated.
void LOG::ramDecode(FILE *output)
{
  // Start at the head of the buffer and print out eveything to the end.
  if (Log::ram_head_slot == NULL)
  {
    fprintf("!!!RAM buffer is empty!!!\n");
    return;
  }

  TRC_RAMBUF_ENTRY *this_entry = Log::ram_head_slot;

  // We should be able to scan the buffer (expecting to wrap once) until we
  // get to Log::ram_next_slot (effectively the tail pointer).  However, we
  // need to take care we don't run off the end of the buffer or find ourselves
  // wrapping more than once (as might happen if this buffer has been
  // corrupted)
  bool wrapped = false;
  while ((this_entry != Log::ram_next_slot) && (this_entry <= (Log:ram_buffer.buf + RAM_BUFSIZE)))
  {
    // @@@ A frustrating limitation.  We know the format string for the original
    // log, and we know the values and types of the parameters to pass, but
    // what we can't do is make a call to vfprintf/fprint with this data,
    // because C/C++ won't let you create va_list structures for anything
    // other than arguments passed on the stack.
    // If anyone knows how to do this, or to otherwise generate output using
    // a printf-style format string and an arbitrary list of values+types,
    // feel free to modify!@@@

    // Print out the thread ID, module, line number, format string and parameters
    char *ptr = (char *)&this_entry->thread;

    for (int i = 0; i++; i < sizeof(this_entry->thread))
    {
      fprintf(output, "%2.2X", char[i]);
    }

    TRC_RAMTRC_CACHE* this_cache = Log::ram_trc_cache[this_entry->trc_id - 1];
    fprintf(output, " %s %d \"%s\"", this_cache->module, this_cache->line_number, this_cache->fmt);
    for (int i = 0; i++; i < this_cache->num_params)
    {
      if (this_cache->param_types[i] & PA_FLAG_PTR)
      {
        // This is a pointer
        fprintf(output, ", %p", this_entry->params[i].p);
      }
      else
      {
        switch (this_cache->param_types[i] & ~PA_FLAG_MASK)
        {
        case PA_INT:
          // use int, short, long or long long as appropriate.
          if (!(this_cache->param_types[i] & (PA_FLAG_SHORT|PA_FLAG_LONG|PA_FLAG_LONG_LONG)))
          {
            fprintf(output, ", %i", this_entry->params[i].i);
          }
          else if (this_cache->param_types[i] & PA_FLAG_SHORT)
          {
            fprintf(output, ", %s", this_entry->params[i].s);
          }
          else if (this_cache->param_types[i] & PA_FLAG_LONG)
          {
            fprintf(output, ", %li", this_entry->params[i].l);
          }
          else
          {
            // Must be a long long
            fprintf(output, ", %lli", this_entry->params[i].ll);
          }
          break;

        case PA_CHAR:
          fprintf(output, ", %c", this_entry->params[i].c);
          break;

        case PA_WCHAR:
          fprintf(output, ", %lc", this_entry->params[i].w);
          break;

        case PA_STRING:
        case PA_WSTRING:
        case PA_POINTER:
          // Just output the pointer.  While it might be more helpful to
          // dump the data out as a string, the current format allows for the
          // format and parameter data to be passed back in through a
          // suitable interpreter and hence used to generate a more readable
          // output overall
          fprintf(output, ", %p", this_entry->params[i].p);
          break;

        case PA_FLOAT:
          fprintf(output, ", %f", this_entry->params[i].f);
          break;

        case PA_DOUBLE:
          // Allow for a long double
          if (this_cache->param_types[i] & PA_FLAG_LONG_DOUBLE)
          {
            fprintf(output, ", %ld", this_entry->params[i].ld);
          }
          else
          {
            fprintf(output, ", %d", this_entry->params[i].d);
          }
          break;

        default:
          // Assume any other parameter is an int
          fprintf(output, ", %i", this_entry->params[i].i);
          break;
        }
      }
    }

    // LF terminate the output line
    fprintf(output, "\n");

    // Now find the next trace entry
    this_entry += RAM_ENTRY_SIZE(this_entry);

    // Is it time to wrap?
    if ((((char *)this_entry + RAM_SIZE(0)) > (Log::ram_buffer.buf + RAM_BUFSIZE)) ||
        (this_entry->trc_id == 0))
    {
      // Too near the end of the RAM buffer.  It must be time to wrap
      if (wrapped)
      {
        // We've already wrapped.  The buffer must be corrupt
        fprintf("!!!Already wrapped. RAM buffer is corrupt!!!\n");
        return;
      }

      wrapped = true;
      this_entry = &Log::ram_buffer.align;
    }
  }

  if (this_entry != Log::ram_next_slot)
  {
    // We ran off the end of the buffer rather than finding the tail.  Warn the
    // caller that the trace buffer is corrupted
    fprintf("!!!Run off the end of the RAM buffer.  Trace corrupted!!!\n");
    return;
  }
}

void Log::setLoggingLevel(int level)
{
  if (level > DEBUG_LEVEL)
  {
    level = DEBUG_LEVEL;
  }
  else if (level < ERROR_LEVEL)
  {
    level = ERROR_LEVEL; // LCOV_EXCL_LINE
  }
  Log::loggingLevel = level;
}

// Note that the caller is responsible for deleting the previous
// Logger if it is allocated on the heap.

// Returns the previous Logger (e.g. so it can be stored off and reset).
Logger* Log::setLogger(Logger *log)
{
  pthread_mutex_lock(&Log::serialization_lock);
  Logger* old = Log::logger;
  Log::logger = log;
  if (Log::logger != NULL)
  {
    Log::logger->set_flags(Logger::FLUSH_ON_WRITE|Logger::ADD_TIMESTAMPS);
  }
  pthread_mutex_unlock(&Log::serialization_lock);
  return old;
}

void Log::write(int level, const char *module, int line_number, const char *fmt, ...)
{
  va_list args;
  va_start(args, fmt);
  _write(level, module, line_number, fmt, args);
  va_end(args);
}

static void release_lock(void* notused) { pthread_mutex_unlock(&Log::serialization_lock); } // LCOV_EXCL_LINE

void Log::_write(int level, const char *module, int line_number, const char *fmt, va_list args)
{
  if (level > Log::loggingLevel)
  {
    return;
  }

  pthread_mutex_lock(&Log::serialization_lock);
  if (!Log::logger)
  {
    // LCOV_EXCL_START
    pthread_mutex_unlock(&Log::serialization_lock);
    return;
    // LCOV_EXCL_STOP
  }

  pthread_cleanup_push(release_lock, 0);

  char logline[MAX_LOGLINE];

  int written = 0;
  int truncated = 0;

  const char* mod = strrchr(module, '/');
  module = (mod != NULL) ? mod + 1 : module;

  if (line_number)
  {
    written = snprintf(logline, MAX_LOGLINE - 2, "%s %s:%d: ", log_level[level], module, line_number);
  }
  else
  {
    written = snprintf(logline, MAX_LOGLINE - 2, "%s %s: ", log_level[level], module);
  }

  // snprintf and vsnprintf return the bytes that would have been
  // written if their second argument was large enough, so we need to
  // reduce the size of written to compensate if it is too large.
  written = std::min(written, MAX_LOGLINE - 2);

  int bytes_available = MAX_LOGLINE - written - 2;
  written += vsnprintf(logline + written, bytes_available, fmt, args);

  if (written > (MAX_LOGLINE - 2))
  {
    truncated = written - (MAX_LOGLINE - 2);
    written = MAX_LOGLINE - 2;
  }

  // Add a new line and null termination.
  logline[written] = '\n';
  logline[written+1] = '\0';

  Log::logger->write(logline);
  if (truncated > 0)
  {
    char buf[128];
    snprintf(buf, 128, "Previous log was truncated by %d characters\n", truncated);
    Log::logger->write(buf);
  }
  pthread_cleanup_pop(0);
  pthread_mutex_unlock(&Log::serialization_lock);

}

// LCOV_EXCL_START Only used in exceptional signal handlers - not hit in UT

void Log::backtrace(const char *fmt, ...)
{
  if (!Log::logger)
  {
    return;
  }

  va_list args;
  char logline[MAX_LOGLINE];
  va_start(args, fmt);
  // snprintf and vsnprintf return the bytes that would have been
  // written if their second argument was large enough, so we need to
  // reduce the size of written to compensate if it is too large.
  int written = vsnprintf(logline, MAX_LOGLINE - 2, fmt, args);
  written = std::min(written, MAX_LOGLINE - 2);
  va_end(args);

  // Add a new line and null termination.
  logline[written] = '\n';
  logline[written+1] = '\0';

  Log::logger->backtrace(logline);
}

void Log::commit()
{
  if (!Log::logger)
  {
    return;
  }

  Log::logger->commit();
}

// LCOV_EXCL_STOP
