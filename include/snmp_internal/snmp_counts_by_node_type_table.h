/**
 * @file snmp_counts_by_node_type_table.h
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

#ifndef SNMP_COUNTS_BY_NODE_TYPE_TABLE_H
#define SNMP_COUNTS_BY_NODE_TYPE_TABLE_H

#include "snmp_internal/snmp_time_period_and_node_type_table.h"

namespace SNMP
{

template <class T, int i> class CountsByNodeTypeTableImpl: public ManagedTable<T, int>
{
public:
  CountsByNodeTypeTableImpl(std::string name,
                            std::string tbl_oid):
    ManagedTable<T, int>(name,
                                         tbl_oid,
                                         3,
                                         3 + i,
                                         { ASN_INTEGER , ASN_INTEGER }) // Types of the index columns
  {
    int n = 0;
    std::vector<NodeTypes> nodes = { NodeTypes::SCSCF, NodeTypes::ICSCF, NodeTypes::BGCF };

    for (std::vector<NodeTypes>::iterator node_type = nodes.begin();
         node_type != nodes.end();
         node_type++)
    {
      five_second[*node_type] = new typename T::CurrentAndPrevious(5);
      five_minute[*node_type] = new typename T::CurrentAndPrevious(300);

      this->add(n++, new T(TimePeriodIndexes::scopePrevious5SecondPeriod, *node_type, new typename T::PreviousView(five_second[*node_type])));
      this->add(n++, new T(TimePeriodIndexes::scopeCurrent5MinutePeriod, *node_type, new typename T::CurrentView(five_minute[*node_type])));
      this->add(n++, new T(TimePeriodIndexes::scopePrevious5MinutePeriod, *node_type, new typename T::PreviousView(five_minute[*node_type])));
    }
  }

protected:
  T* new_row(int indexes) { return NULL; };

  std::map<NodeTypes, typename T::CurrentAndPrevious*> five_second;
  std::map<NodeTypes, typename T::CurrentAndPrevious*> five_minute;
};

template <class T, int i> class CountsByNodeTypeTable
{
public:
  virtual ~CountsByNodeTypeTable() {};

  CountsByNodeTypeTable* create(std::string name,
                                std::string tbl_oid)
  {
    return new CountsByNodeTypeTableImpl<T, i>(name, tbl_oid);
  }

  CountsByNodeTypeTable() {};
};

}

#endif
