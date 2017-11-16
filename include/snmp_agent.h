/**
 * @file snmp_agent.h Initialization and termination functions for Sprout SNMP.
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

#include <string>
#include "snmp_internal/snmp_includes.h"

#ifndef CW_SNMP_AGENT_H
#define CW_SNMP_AGENT_H

namespace SNMP
{

class Agent
{
public:
  // Create a new instance - not thread safe. Will delete any existing instances, which must no
  // longer be in use.
  static void instantiate(std::string name);

  // Destroy the instance - not thread safe. The instance must no longer be in use.
  static void deinstantiate();

  static inline Agent* instance() { return _instance; }

  void start(void);
  void stop(void);
  void add_row_to_table(netsnmp_tdata* table, netsnmp_tdata_row* row);
  void remove_row_from_table(netsnmp_tdata* table, netsnmp_tdata_row* row);

private:
  static Agent* _instance;
  std::string _name;
  pthread_t _thread;
  pthread_mutex_t _netsnmp_lock = PTHREAD_MUTEX_INITIALIZER;

  Agent(std::string name);
  ~Agent();

  static void* thread_fn(void* snmp_handler);
  void thread_fn(void);
  static int logging_callback(int majorID, int minorID, void* serverarg, void* clientarg);
};

}

// Starts the SNMP agent thread. 'name' is passed through to the netsnmp library as the application
// name - this is arbitrary, but should be spomething sensible (e.g. 'sprout', 'bono').
int snmp_setup(const char* name);

int init_snmp_handler_threads(const char* name);

// Terminates the SNMP agent thread. 'name' should match the string passed to snmp_setup.
void snmp_terminate(const char* name);

#endif
