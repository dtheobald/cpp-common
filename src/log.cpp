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

typedef struct
{
  pthread_t   thread;
  int         line_number;
  const char *module;
  const char *fmt;
  int         num_params;
  va_arg      params[1];

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
}

// Fast RAM buffer write routine
void LOG::ramTrace(const char *module, int line_number, int num_params, char *fmt, ...)
{
  va_list args;
  va_start(args, fmt);

  // Lock access to the RAM buffer

  pthread_mutex_lock(&Log::ram_lock);

  int entry_size = RAM_SIZE(num_params);

  // Find space in the buffer to write our log entry into
  if (((char *)Log::ram_next_slot + entry_size) > (ram_buffer.buf + RAM_BUFSIZE))
  {
    // The log isn't going to fit at the end of the buffer, so we need to wrap it.
    // Set any remaining space in the buffer to zero (so that it can't be accidentally
    // interpreted as good data).
    memset((char *)Log::ram_next_slot, 0, RAM_BUFSIZE - ((char *)Log::ram_next_slot - ram_buffer.buf));

    // Check whether the head slot is in the space we have just destroyed (in which case this
    // needs to be reset to the start of the buffer).
    if (Log::ram_head_slot >= Log::ram_next_slot)
    {
      Log::ram_head_slot = &Log::ram_buffer.align;
    }

    // Point the next slot at the head of the buffer
    Log::ram_next_slot = &Log::ram_buffer.align;
  }

  // If we have an oldest log in the buffer (i.e., a "head" entry), bump this
  // along if we're about to overwrite it
  while ((Log::ram_head_slot != NULL) && ((char *)Log::ram_next_slot + entry_size) > RAM_ENTRY_SIZE(Log::ram_head_slot))
  {
    Log::ram_head_slot = (char *)Log::ram_head_slot + RAM_ENTRY_SIZE(Log::ram_head_slot);

    if ((((char *)Log::ram_head_slot + sizeof(TRC_RAMBUF_ENTRY)) > (ram_buffer.buf + RAM_BUFSIZE)) ||
        (Log::ram_head_slot->module == NULL))
    {
      // Head slot no longer points to a valid thread entry.  Wrap it and stop looking
      Log::ram_head_slot = &Log::ram_buffer.align;
      break;
    }
  }

  // We now have a slot we can write our log entry into and a head entry
  // which we are not about to overwrite.

  // Save off a pointer to the slot and bump the next_slot pointer past it.
  // Note that it doesn't matter if there isn't enough space in the buffer to
  // write here - the next logging thread will spot this and wrap it
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
  this_slot->module = module;
  this_slot->line_number = line_number;
  this_slot->fmt = fmt;
  this_slot->num_params = num_params;

  for (int i = 0; i < num_args; i++)
  {
    this_slot->params[i] =
  }


  // Release the mutex so that other threads can get in
  pthread_mutex_unlock(&Log::ram_lock);

  va_end(args);
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
