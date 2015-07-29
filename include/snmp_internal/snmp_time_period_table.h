/**
 * @file snmp_latency_table.h
 *
 * Project Clearwater - IMS in the Cloud
 * Copyright (C) 2015 Metaswitch Networks Ltd
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
 * software and licensed und er the OpenSSL Licenses.
 * "OpenSSL Licenses" means the OpenSSL License and Original SSLeay License
 * under which the OpenSSL Project distributes the OpenSSL toolkit software,
 * as those licenses appear in the file LICENSE-OPENSSL.
 */

#include "snmp_table.h"
#include <vector>
#include <map>
#include <string>
#include <atomic>
#include "logger.h"

#ifndef SNMP_TIME_PERIOD_TABLE_H
#define SNMP_TIME_PERIOD_TABLE_H

// This file contains the base infrastructure for SNMP tables which are indexed by time period. It
// contains only abstract classes, which need to be subclassed - e.g. SNMP::AccumulatorRow and
// SNMP::EventAccumulatorTable.


namespace SNMP
{

enum TimePeriodIndexes
{
  scopePrevious5SecondPeriod = 1,
  scopeCurrent5MinutePeriod = 2,
  scopePrevious5MinutePeriod = 3,
};

template <class T> class TimeBasedRow : public Row
{
public:

  // For the data type T, store a current and previous period (duration defined by _interval) of
  // the data. (For example, the current five seconds of data and the previous five seconds of
  // data).
  class CurrentAndPrevious
  {
  public:
    CurrentAndPrevious(int interval):
      current(&a),
      previous(&b),
      _interval(interval), // In s.
      _tick(0),
      a(),
      b()
    {
      struct timespec now;
      clock_gettime(CLOCK_REALTIME_COARSE, &now);
      uint64_t time_now = (now.tv_sec * 1000) + (now.tv_nsec / 1000000);

      a.reset(NULL, time_now);
      b.reset(NULL, time_now-(interval*1000));
    }

    // Rolls the current period over into the previous period if necessary.
    void update_time()
    {
      struct timespec now;
      clock_gettime(CLOCK_REALTIME_COARSE, &now);

      // Count of how many _interval periods have passed since the epoch
      uint32_t new_tick = (now.tv_sec / _interval);

      // Count of how many _interval periods have passed since the last change
      uint32_t tick_difference = new_tick - _tick;
      _tick = new_tick;

      if (tick_difference == 1)
      {
        T* tmp;
        tmp = previous.load();
        previous.store(current);
        tmp->reset(current.load(), new_tick * _interval * 1000);
        current.store(tmp);
      }
      else if (tick_difference > 1)
      {
        current.load()->reset(current.load(), new_tick * _interval * 1000);
        previous.load()->reset(current.load(), (new_tick-1) * _interval * 1000);
      }
    }

    T* get_current() { update_time(); return current.load(); }
    T* get_previous() { update_time(); return previous.load(); }
    uint32_t get_interval() { return _interval; }

  protected:
    std::atomic<T*> current;
    std::atomic<T*> previous;
    uint32_t _interval;
    uint32_t _tick;
    T a;
    T b;

  };

  // A view into a CurrentAndPrevious set of data. See CurrentView and PreviousView.
  class View
  {
  public:
    View(CurrentAndPrevious* data): _data(data) {};
    virtual ~View() {};
    virtual T* get_data() = 0;
    // Return interval in ms
    uint32_t get_interval_ms() { return (this->_data->get_interval()) * 1000; }
  protected:
    CurrentAndPrevious* _data;
  };

  // A view into the current part of a CurrentAndPrevious set of data.
  class CurrentView : public View
  {
  public:
    CurrentView(CurrentAndPrevious* data): View(data) {};
    T* get_data() { return this->_data->get_current(); };
  };

  // A view into the previous part of a CurrentAndPrevious set of data.
  class PreviousView : public View
  {
  public:
    PreviousView(CurrentAndPrevious* data): View(data) {};
    T* get_data() { return this->_data->get_previous(); };
  };


  // Finished with inner classes, back into TimeBasedRow.

  // Constructor, takes ownership of the View*.
  TimeBasedRow(int index, View* view) :
    Row(),
    _index(index),
    _view(view)
  {
    // Time-based rows are indexed off a single integer representing the time period.
    netsnmp_tdata_row_add_index(_row,
                                ASN_INTEGER,
                                &_index,
                                sizeof(int));

  };

  virtual ~TimeBasedRow()
  {
    delete(_view);
  };

  virtual ColumnData get_columns() = 0;

protected:
  uint32_t _index;
  View* _view;
};

}

#endif