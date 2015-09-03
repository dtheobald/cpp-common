/**
 * @file snmp_cx_counter_table.cpp
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
 * software and licensed under the OpenSSL Licenses.
 * "OpenSSL Licenses" means the OpenSSL License and Original SSLeay License
 * under which the OpenSSL Project distributes the OpenSSL toolkit software,
 * as those licenses appear in the file LICENSE-OPENSSL.
 */

#include "snmp_internal/snmp_includes.h"
#include "snmp_internal/snmp_time_period_and_double_indexed_table.h"
#include "snmp_cx_counter_table.h"
#include <vector>
#include "logger.h"

namespace SNMP
{

// The Diameter base protocol result codes as documented in RFC 6733.
static std::vector<int> base_result_codes =
{
  1001,
  2001, 2002,
  3001, 3002, 3003, 3004, 3005, 3006, 3007, 3008, 3009, 3010,
  4001, 4002, 4003,
  5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010, 5011, 5012, 5013,
  5014, 5015, 5016, 5017
};

// The 3GPP specific result codes as documented in ETSI TS 129 229.
static std::vector<int> _3gpp_result_codes =
{
  2001, 2002, 2003, 2004,
  5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5011
};


// Storage for the underlying data
struct SingleCount
{
  uint_fast32_t count;
  void reset(uint64_t time_periodstart, SingleCount* previous = NULL) { count = 0; };
};

// Time and Double Indexed Row that maps the data from SingleCount into the right column.
class CxCounterRow: public TimeAndDoubleIndexedRow<SingleCount>
{
public:
  CxCounterRow(int time_index, DiameterAppId app_id, int result_code, View* view):
    TimeAndDoubleIndexedRow<SingleCount>(time_index, app_id, result_code, view) {};

  ColumnData get_columns()
  {
    struct timespec now;
    clock_gettime(CLOCK_REALTIME_COARSE, &now);

    SingleCount count = *(this->_view->get_data(now));

    // Construct and return a ColumnData with the appropriate values
    ColumnData ret;
    ret[1] = Value::integer(this->_index);
    ret[2] = Value::integer(this->_first_index);
    ret[3] = Value::integer(this->_second_index);
    ret[4] = Value::uint(count.count);
    return ret;
  }

};

class CxCounterTableImpl: public ManagedTable<CxCounterRow, int>, public CxCounterTable
{
public:
  CxCounterTableImpl(std::string name,
                     std::string tbl_oid):
    ManagedTable<CxCounterRow, int>(name,
                                    tbl_oid,
                                    4,
                                    4,
                                    { ASN_INTEGER , ASN_INTEGER , ASN_INTEGER })
  {
    n = 0;

    for (std::vector<int>::iterator code = base_result_codes.begin();
         code != base_result_codes.end();
         code++)
    {
      create_and_add_rows(DiameterAppId::BASE, *code);
    }

    for (std::vector<int>::iterator code = _3gpp_result_codes.begin();
        code != _3gpp_result_codes.end();
        code++)
    {
      create_and_add_rows(DiameterAppId::_3GPP, *code);
    }

    create_and_add_rows(DiameterAppId::TIMEOUT, 0);
  }

  void create_and_add_rows(DiameterAppId app_id, int code)
  {
    std::map<int, CurrentAndPrevious<SingleCount>*>* five_second;
    std::map<int, CurrentAndPrevious<SingleCount>*>* five_minute;

    switch (app_id)
    {
      case BASE:
        five_second = &base_five_second;
        five_minute = &base_five_minute;
        break;
      case _3GPP:
        five_second = &_3gpp_five_second;
        five_minute = &_3gpp_five_minute;
        break;
      case TIMEOUT:
        five_second = &timeout_five_second;
        five_minute = &timeout_five_minute;
        break;
    }

    (*five_second)[code] = new CurrentAndPrevious<SingleCount>(5000);
    (*five_minute)[code] = new CurrentAndPrevious<SingleCount>(300000);
    this->add(n++, new CxCounterRow(TimePeriodIndexes::scopePrevious5SecondPeriod,
                                    app_id,
                                    code,
                                    new CxCounterRow::PreviousView((*five_second)[code])));
    this->add(n++, new CxCounterRow(TimePeriodIndexes::scopeCurrent5MinutePeriod,
                                    app_id,
                                    code,
                                    new CxCounterRow::CurrentView((*five_minute)[code])));
    this->add(n++, new CxCounterRow(TimePeriodIndexes::scopePrevious5MinutePeriod,
                                    app_id,
                                    code,
                                    new CxCounterRow::PreviousView((*five_minute)[code])));
  }

  /// Helper function to increment the current counter for this code if we have a bucket for it.
  /// We don't have a bucket for all counters, so we need to check first.
  void safe_increment_current(std::map<int, CurrentAndPrevious<SingleCount>*>& map, int code)
  {
    if (map.count(code) != 0)
    {
      map[code]->get_current()->count++;
    }
  }

  void increment(DiameterAppId app_id, int result_code)
  {
    switch (app_id)
    {
      case BASE:
        safe_increment_current(base_five_second, result_code);
        safe_increment_current(base_five_minute, result_code);
        break;
      case _3GPP:
        safe_increment_current(_3gpp_five_second, result_code);
        safe_increment_current(_3gpp_five_minute, result_code);
        break;
      case TIMEOUT:
        safe_increment_current(timeout_five_second, result_code);
        safe_increment_current(timeout_five_minute, result_code);
        break;
    }
  }

  ~CxCounterTableImpl()
  {
    for (std::map<int, CurrentAndPrevious<SingleCount>*>::iterator type = base_five_second.begin();
         type != base_five_second.end();
         type++)
    {  delete type->second; }
    for (std::map<int, CurrentAndPrevious<SingleCount>*>::iterator type = base_five_minute.begin();
         type != base_five_minute.end();
         type++)
    {  delete type->second; }

    for (std::map<int, CurrentAndPrevious<SingleCount>*>::iterator type = _3gpp_five_second.begin();
         type != _3gpp_five_second.end();
         type++)
    {  delete type->second; }
    for (std::map<int, CurrentAndPrevious<SingleCount>*>::iterator type = _3gpp_five_minute.begin();
         type != _3gpp_five_minute.end();
         type++)
    {  delete type->second; }

    delete timeout_five_second.begin()->second;
    delete timeout_five_minute.begin()->second;
  }
private:

  CxCounterRow* new_row(int indexes) { return NULL;};

  int n;

  std::map<int, CurrentAndPrevious<SingleCount>*> base_five_second;
  std::map<int, CurrentAndPrevious<SingleCount>*> base_five_minute;

  std::map<int, CurrentAndPrevious<SingleCount>*> _3gpp_five_second;
  std::map<int, CurrentAndPrevious<SingleCount>*> _3gpp_five_minute;

  std::map<int, CurrentAndPrevious<SingleCount>*> timeout_five_second;
  std::map<int, CurrentAndPrevious<SingleCount>*> timeout_five_minute;
};

CxCounterTable* CxCounterTable::create(std::string name,
                                       std::string oid)
{
  return new CxCounterTableImpl(name, oid);
}

}