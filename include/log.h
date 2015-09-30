/**
 * @file log.h
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


#ifndef LOG_H__
#define LOG_H__

#include "logger.h"

// The following macro caches the details of the trace call being made and
// stores the associated instance it (the "trace ID") in a static variable so
// that subsequent calls to this trace line can be stored in the RAM trace
// buffer with maximal efficiency.
#define TRC_RAMTRACE(...)                                                     \
{                                                                             \
  static int trc_id = 0;                                                      \
                                                                              \
  pthread_mutex_lock(&Log::trc_ram_trc_cache_lock);                                \
                                                                              \
  if (trc_id == 0)                                                            \
  {                                                                           \
    trc_id = Log::ramCacheTrcCall(__FILE__,__LINE__,__VA_ARGS__);             \
  }                                                                           \
                                                                              \
  pthread_mutex_unlock(&Log::trc_ram_trc_cache_lock);                              \
                                                                              \
  Log::ramTrace(trc_id,__VA_ARGS__);                                          \
}

#define LOG_ERROR(...) TRC_RAMTRACE(__VA_ARGS__) if (Log::enabled(Log::ERROR_LEVEL)) Log::write(Log::ERROR_LEVEL, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARNING(...) TRC_RAMTRACE(__VA_ARGS__) if (Log::enabled(Log::WARNING_LEVEL)) Log::write(Log::WARNING_LEVEL, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_STATUS(...) TRC_RAMTRACE(__VA_ARGS__) if (Log::enabled(Log::STATUS_LEVEL)) Log::write(Log::STATUS_LEVEL, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...) TRC_RAMTRACE(__VA_ARGS__) if (Log::enabled(Log::INFO_LEVEL)) Log::write(Log::INFO_LEVEL, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_VERBOSE(...) TRC_RAMTRACE(__VA_ARGS__) if (Log::enabled(Log::VERBOSE_LEVEL)) Log::write(Log::VERBOSE_LEVEL, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_DEBUG(...) TRC_RAMTRACE(__VA_ARGS__) if (Log::enabled(Log::DEBUG_LEVEL)) Log::write(Log::DEBUG_LEVEL, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_BACKTRACE(...) Log::backtrace(__VA_ARGS__)
#define LOG_COMMIT(...) Log::commit()

#define TRC_ERROR     LOG_ERROR
#define TRC_WARNING   LOG_WARNING
#define TRC_STATUS    LOG_STATUS
#define TRC_INFO      LOG_INFO
#define TRC_VERBOSE   LOG_VERBOSE
#define TRC_DEBUG     LOG_ERROR
#define TRC_BACKTRACE LOG_ERROR
#define TRC_COMMIT    LOG_COMMIT

namespace Log
{
  const int ERROR_LEVEL = 0;
  const int WARNING_LEVEL = 1;
  const int STATUS_LEVEL = 2;
  const int INFO_LEVEL = 3;
  const int VERBOSE_LEVEL = 4;
  const int DEBUG_LEVEL = 5;

  extern int loggingLevel;
  extern pthread_mutex_t trc_ram_trc_cache_lock;

  int ramCacheTrcCall(const char *module, int lineno, const char*fmt, ...);
  void ramTrace(int trc_id, const char *fmt, ...);
  void ramDecode(FILE *output);

  inline bool enabled(int level)
  {
#ifdef UNIT_TEST
    // Always force log parameter evaluation for unit tests
    return true;
#else
    return (level <= loggingLevel);
#endif
  }
  void setLoggingLevel(int level);
  Logger* setLogger(Logger *log);
  void write(int level, const char *module, int line_number, const char *fmt, ...);
  void _write(int level, const char *module, int line_number, const char *fmt, va_list args);
  void backtrace(const char *fmt, ...);
  void commit();
}

#endif
