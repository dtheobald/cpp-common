/**
 * @file httpconnection.cpp HttpConnectionLite class methods.
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

#include <cassert>
#include <iostream>
#include <map>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>

#include "cpp_common_pd_definitions.h"
#include "utils.h"
#include "log.h"
#include "sas.h"
#include "httpconnectionlite.h"
#include "load_monitor.h"
#include "random_uuid.h"
#include "httpresponseparser.h"

/// Total time to wait for a response from the server as a multiple of the
/// configured target latency before giving up.  This is the value that affects
/// the user experience, so should be set to what we consider acceptable.
/// Covers lookup, possibly multiple connection attempts, request, and
/// response.
static const int TIMEOUT_LATENCY_MULTIPLIER = 5;
static const int DEFAULT_LATENCY_US = 100000;

/// Approximate length of time to wait before giving up on a
/// connection attempt to a single address (in milliseconds).  cURL
/// may wait more or less than this depending on the number of
/// addresses to be tested and where this address falls in the
/// sequence. A connection will take longer than this to establish if
/// multiple addresses must be tried. This includes only the time to
/// perform the DNS lookup and establish the connection, not to send
/// the request or receive the response.
///
/// We set this quite short to ensure we quickly move on to another
/// server. A connection should be very fast to establish (a few
/// milliseconds) in the success case.
static const long SINGLE_CONNECT_TIMEOUT_MS = 50;

/// Mean age of a connection before we recycle it. Ensures we respect
/// DNS changes, and that we rebalance load when servers come back up
/// after failure. Actual connection recycle events are
/// Poisson-distributed with this mean inter-arrival time.
static const double CONNECTION_AGE_MS = 60 * 1000.0;

/// Maximum number of targets to try connecting to.
static const int MAX_TARGETS = 5;

/// Create an HTTP connection object.
///
/// @param server Server to send HTTP requests to.
/// @param assert_user Assert user in header?
/// @param resolver HTTP resolver to use to resolve server's IP addresses
/// @param stat_name SNMP table to report connection info to.
/// @param load_monitor Load Monitor.
/// @param sas_log_level the level to log HTTP flows at (none/protocol/detail).
HttpConnectionLite::HttpConnectionLite(const std::string& server,
                                       bool assert_user,
                                       HttpResolver* resolver,
                                       SNMP::IPCountTable* stat_table,
                                       LoadMonitor* load_monitor,
                                       SASEvent::HttpLogLevel sas_log_level,
                                       BaseCommunicationMonitor* comm_monitor) :
  _server(server),
  _host(host_from_server(server)),
  _port(port_from_server(server)),
  _assert_user(assert_user),
  _resolver(resolver),
  _sas_log_level(sas_log_level),
  _comm_monitor(comm_monitor),
  _stat_table(stat_table)
{
  pthread_key_create(&_conn_thread_local, cleanup_conn);
  pthread_key_create(&_uuid_thread_local, cleanup_uuid);
  pthread_mutex_init(&_lock, NULL);
  std::vector<std::string> no_stats;
  _load_monitor = load_monitor;
  _timeout_ms = calc_req_timeout_from_latency((load_monitor != NULL) ?
                                              load_monitor->get_target_latency_us() :
                                              DEFAULT_LATENCY_US);
  TRC_STATUS("Configuring HTTP Connection");
  TRC_STATUS("  Connection created for server %s", _server.c_str());
  TRC_STATUS("  Connection will use a response timeout of %ldms", _timeout_ms);
}


/// Create an HTTP connection object.
///
/// @param server Server to send HTTP requests to.
/// @param assert_user Assert user in header?
/// @param resolver HTTP resolver to use to resolve server's IP addresses
/// @param sas_log_level the level to log HTTP flows at (none/protocol/detail).
HttpConnectionLite::HttpConnectionLite(const std::string& server,
                                       bool assert_user,
                                       HttpResolver* resolver,
                                       SASEvent::HttpLogLevel sas_log_level,
                                       BaseCommunicationMonitor* comm_monitor) :
  _server(server),
  _host(host_from_server(server)),
  _port(port_from_server(server)),
  _assert_user(assert_user),
  _resolver(resolver),
  _sas_log_level(sas_log_level),
  _comm_monitor(comm_monitor),
  _stat_table(NULL)
{
  pthread_key_create(&_conn_thread_local, cleanup_conn);
  pthread_key_create(&_uuid_thread_local, cleanup_uuid);
  pthread_mutex_init(&_lock, NULL);
  _load_monitor = NULL;
  _timeout_ms = calc_req_timeout_from_latency(DEFAULT_LATENCY_US);
  TRC_STATUS("Configuring HTTP Connection");
  TRC_STATUS("  Connection created for server %s", _server.c_str());
  TRC_STATUS("  Connection will use a response timeout of %ldms", _timeout_ms);
}

HttpConnectionLite::~HttpConnectionLite()
{
  // Clean up this thread's connection now, rather than waiting for
  // pthread_exit.  This is to support use by single-threaded code
  // (e.g., UTs), where pthread_exit is never called.
  Connection* conn = (Connection*)pthread_getspecific(_conn_thread_local);
  if (conn != NULL)
  {
    pthread_setspecific(_conn_thread_local, NULL);
    cleanup_conn(conn); conn = NULL;
  }

  RandomUUIDGenerator* uuid_gen =
    (RandomUUIDGenerator*)pthread_getspecific(_uuid_thread_local);

  if (uuid_gen != NULL)
  {
    pthread_setspecific(_uuid_thread_local, NULL);
    cleanup_uuid(uuid_gen); uuid_gen = NULL;
  }

  pthread_key_delete(_conn_thread_local);
  pthread_key_delete(_uuid_thread_local);
}

/// Get the thread-local connection if it exists, and create it if not.
HttpConnectionLite::Connection* HttpConnectionLite::get_connection()
{
  Connection* conn = (Connection*)pthread_getspecific(_conn_thread_local);
  if (conn == NULL)
  {
    conn = new Connection(this, _sas_log_level);
    pthread_setspecific(_conn_thread_local, conn);
  }
  return conn;
}

HTTPCode HttpConnectionLite::send_delete(const std::string& path,
                                         SAS::TrailId trail,
                                         const std::string& body)
{
  std::string unused_response;
  std::map<std::string, std::string> unused_headers;

  return send_delete(path, unused_headers, unused_response, trail, body);
}

HTTPCode HttpConnectionLite::send_delete(const std::string& path,
                                         SAS::TrailId trail,
                                         const std::string& body,
                                         const std::string& override_server)
{
  std::string unused_response;
  std::map<std::string, std::string> unused_headers;
  change_server(override_server);

  return send_delete(path, unused_headers, unused_response, trail, body);
}

HTTPCode HttpConnectionLite::send_delete(const std::string& path,
                                         SAS::TrailId trail,
                                         const std::string& body,
                                         std::string& response)
{
  std::map<std::string, std::string> unused_headers;

  return send_delete(path, unused_headers, response, trail, body);
}

HTTPCode HttpConnectionLite::send_delete(const std::string& path,
                                         std::map<std::string, std::string>& headers,
                                         std::string& response,
                                         SAS::TrailId trail,
                                         const std::string& body,
                                         const std::string& username)
{
  std::vector<std::string> extra_headers;
  return send_request(path,
                      body,
                      response,
                      username,
                      trail,
                      "DELETE",
                      extra_headers,
                      NULL);
}

HTTPCode HttpConnectionLite::send_put(const std::string& path,
                                      const std::string& body,
                                      SAS::TrailId trail,
                                      const std::string& username)
{
  std::string unused_response;
  std::map<std::string, std::string> unused_headers;
  std::vector<std::string> extra_req_headers;
  return send_put(path,
                  unused_headers,
                  unused_response,
                  body,
                  extra_req_headers,
                  trail,
                  username);
}

HTTPCode HttpConnectionLite::send_put(const std::string& path,
                                      std::string& response,
                                      const std::string& body,
                                      SAS::TrailId trail,
                                      const std::string& username)
{
  std::map<std::string, std::string> unused_headers;
  std::vector<std::string> extra_req_headers;
  return send_put(path,
                  unused_headers,
                  response,
                  body,
                  extra_req_headers,
                  trail,
                  username);
}

HTTPCode HttpConnectionLite::send_put(const std::string& path,
                                      std::map<std::string, std::string>& headers,
                                      const std::string& body,
                                      SAS::TrailId trail,
                                      const std::string& username)
{
  std::string unused_response;
  std::vector<std::string> extra_req_headers;
  return send_put(path,
                  headers,
                  unused_response,
                  body,
                  extra_req_headers,
                  trail,
                  username);
}

HTTPCode HttpConnectionLite::send_put(const std::string& path,
                                      std::map<std::string, std::string>& headers,
                                      std::string& response,
                                      const std::string& body,
                                      const std::vector<std::string>& extra_req_headers,
                                      SAS::TrailId trail,
                                      const std::string& username)
{
  return send_request(path,
                      body,
                      response,
                      "",
                      trail,
                      "PUT",
                      extra_req_headers,
                      &headers);
}

HTTPCode HttpConnectionLite::send_post(const std::string& path,
                                       std::map<std::string, std::string>& headers,
                                       const std::string& body,
                                       SAS::TrailId trail,
                                       const std::string& username)
{
  std::string unused_response;
  return send_post(path, headers, unused_response, body, trail, username);
}

HTTPCode HttpConnectionLite::send_post(const std::string& path,
                                       std::map<std::string, std::string>& headers,
                                       std::string& response,
                                       const std::string& body,
                                       SAS::TrailId trail,
                                       const std::string& username)
{
  std::vector<std::string> unused_extra_headers;

  return send_request(path,
                      body,
                      response,
                      username,
                      trail,
                      "POST",
                      unused_extra_headers,
                      &headers);
}

/// Get data; return a HTTP return code
HTTPCode HttpConnectionLite::send_get(const std::string& path,
                                      std::string& response,
                                      const std::string& username,
                                      SAS::TrailId trail)
{
  std::map<std::string, std::string> unused_rsp_headers;
  std::vector<std::string> unused_req_headers;
  return send_get(path,
                  unused_rsp_headers,
                  response,
                  username,
                  unused_req_headers,
                  trail);
}

/// Get data; return a HTTP return code
HTTPCode HttpConnectionLite::send_get(const std::string& path,
                                      std::string& response,
                                      std::vector<std::string> headers,
                                      const std::string& override_server,
                                      SAS::TrailId trail)
{
  change_server(override_server);

  std::map<std::string, std::string> unused_rsp_headers;
  return send_get(path, unused_rsp_headers, response, "", headers, trail);
}

/// Get data; return a HTTP return code
HTTPCode HttpConnectionLite::send_get(const std::string& path,
                                      std::map<std::string, std::string>& headers,
                                      std::string& response,
                                      const std::string& username,
                                      SAS::TrailId trail)
{
  std::vector<std::string> unused_req_headers;
  return send_get(path, headers, response, username, unused_req_headers, trail);
}

/// Get data; return a HTTP return code
HTTPCode HttpConnectionLite::send_get(const std::string& path,                     //< Absolute path to request from server - must start with "/"
                                      std::map<std::string, std::string>& headers, //< Map of headers from the response
                                      std::string& response,                       //< Retrieved document
                                      const std::string& username,                 //< Username to assert (if assertUser was true, else ignored)
                                      std::vector<std::string> headers_to_add,     //< Extra headers to add to the request
                                      SAS::TrailId trail)                          //< SAS trail
{
  return send_request(path, "", response, username, trail, "GET", headers_to_add, NULL);
}

/// Get data; return a HTTP return code
HTTPCode HttpConnectionLite::send_request(const std::string& path,                 //< Absolute path to request from server - must start with "/"
                                          std::string body,                        //< Body to send on the request
                                          std::string& doc,                        //< OUT: Retrieved document
                                          const std::string& username,             //< Username to assert (if assertUser was true, else ignored).
                                          SAS::TrailId trail,                      //< SAS trail to use
                                          const std::string& method_str,           // The method, used for logging.
                                          std::vector<std::string> headers_to_add, //< Extra headers to add to the request
                                          std::map<std::string, std::string>* response_headers)
{
  bool ok = false;
  HTTPCode http_code = HTTP_SERVER_ERROR;
  Connection* conn = get_connection();

  // Create a UUID to use for SAS correlation and add it to the HTTP message.
  boost::uuids::uuid uuid = get_random_uuid();
  std::string uuid_str = boost::uuids::to_string(uuid);
  headers_to_add.push_back(SASEvent::HTTP_BRANCH_HEADER_NAME + ": " + uuid_str);

  // Now log the marker to SAS. Flag that SAS should not reactivate the trail
  // group as a result of associations on this marker (doing so after the call
  // ends means it will take a long time to be searchable in SAS).
  SAS::Marker corr_marker(trail, MARKER_ID_VIA_BRANCH_PARAM, 0);
  corr_marker.add_var_param(uuid_str);
  SAS::report_marker(corr_marker, SAS::Marker::Scope::Trace, false);

  // Add the user's identity (if required).
  if (_assert_user)
  {
    headers_to_add.push_back("X-XCAP-Asserted-Identity: " + username);
  }

  // Determine whether to recycle the connection, based on
  // previously-calculated deadline.
  struct timespec tp;
  int rv = clock_gettime(CLOCK_MONOTONIC, &tp);
  assert(rv == 0);
  unsigned long now_ms = tp.tv_sec * 1000 + (tp.tv_nsec / 1000000);
  bool recycle_conn = conn->is_connection_expired(now_ms);

  // Resolve the host.
  std::vector<AddrInfo> targets;
  _resolver->resolve(_host, _port, MAX_TARGETS, targets, trail);

  // If we're not recycling the connection, try to get the current connection
  // IP address and add it to the front of the target list (if it was there)
  if (!recycle_conn)
  {
    AddrInfo ai;

    if (conn->get_remote_ai(ai))
    {
      int initialSize = targets.size();
      targets.erase(std::remove(targets.begin(), targets.end(), ai), targets.end());
      if ((int)targets.size() < initialSize)
      {
        targets.insert(targets.begin(), ai);
      }
    }
  }

  // If the list of targets only contains 1 target, clone it - we always want
  // to retry at least once.
  if (targets.size() == 1)
  {
    targets.push_back(targets[0]);
  }

  // Track the number of HTTP 503 and 504 responses and the number of timeouts
  // or I/O errors.
  int num_http_503_responses = 0;
  int num_http_504_responses = 0;
  int num_timeouts_or_io_errors = 0;

  // Track the IP addresses we're connecting to.
  const char *remote_ip = NULL;
  char buf[100];

  // Try to get a decent connection - try each of the hosts in turn (although
  // we might quit early if we have too many HTTP-level failures).
  for (std::vector<AddrInfo>::const_iterator i = targets.begin();
       i != targets.end();
       ++i)
  {
    // Work out the remote IP for logging.
    remote_ip = inet_ntop(i->address.af, &i->address.addr, buf, sizeof(buf));

    // Send the request.
    doc.clear();
    TRC_DEBUG("Sending HTTP request : %s (trying %s) %s",
              path.c_str(),
              remote_ip,
              (recycle_conn) ? "on new connection" : "");

    bool got_response = conn->send_request_recv_response(*i,
                                                         recycle_conn,
                                                         trail,
                                                         method_str,
                                                         path,
                                                         headers_to_add,
                                                         body,
                                                         http_code,
                                                         response_headers,
                                                         &doc);

    // Update the connection recycling and retry algorithms.
    if (got_response && !(http_code >= 400))
    {
      if (recycle_conn)
      {
        conn->update_deadline(now_ms);
      }

      // Success!
      ok = true;
      break;
    }
    else
    {
      // If we forced a new connection and we failed even to establish an HTTP
      // connection, blacklist this IP address.
      if (recycle_conn)
      {
        _resolver->blacklist(*i);
      }

      // Determine the failure mode and update the correct counter.
      bool fatal_http_error = false;
      if (got_response)
      {
        if (http_code >= 400)
        {
          if (http_code == 503)
          {
            num_http_503_responses++;
          }
          else if (http_code == 504)
          {
            num_http_504_responses++;
          }
          else
          {
            fatal_http_error = true;
          }
        }
      }
      else
      {
        num_timeouts_or_io_errors++;
      }

      // Decide whether to keep trying.
      if ((num_http_503_responses + num_timeouts_or_io_errors >= 2) ||
          (num_http_504_responses >= 1) ||
          fatal_http_error)
      {
        // Make a SAS log so that its clear that we have stopped retrying
        // deliberately.
        HttpErrorResponseTypes reason = fatal_http_error ?
          HttpErrorResponseTypes::Permanent :
          HttpErrorResponseTypes::Temporary;
        sas_log_http_abort(trail, reason, 0);
        break;
      }
    }
  }

  // Check whether we should apply a penalty. We do this when:
  //  - both attempts return 503 errors, which means the downstream node is
  //    overloaded/requests to it are timeing.
  //  - the error is a 504, which means that the node downsteam of the node
  //    we're connecting to currently has reported that it is overloaded/was
  //    unresponsive.
  if (((num_http_503_responses >= 2) ||
       (num_http_504_responses >= 1)) &&
      (_load_monitor != NULL))
  {
    _load_monitor->incr_penalties();
  }

  if (ok)
  {
    conn->set_remote_ip(remote_ip);

    if (_comm_monitor)
    {
      // If both attempts fail due to overloaded downstream nodes, consider
      // it a communication failure.
      if (num_http_503_responses >= 2)
      {
        _comm_monitor->inform_failure(now_ms); // LCOV_EXCL_LINE - No UT for 503 fails
      }
      else
      {
        _comm_monitor->inform_success(now_ms);
      }
    }
  }
  else
  {
    conn->set_remote_ip("");

    if (_comm_monitor)
    {
      _comm_monitor->inform_failure(now_ms);
    }
  }

  return http_code;
}

/// Called to clean up the connection handle.
void HttpConnectionLite::cleanup_conn(void* ptr)
{
  Connection* conn = (Connection*)ptr;
  conn->set_remote_ip("");
  delete conn; conn = NULL;
}


/// Connection constructor
HttpConnectionLite::Connection::Connection(HttpConnectionLite* parent,
                                           SASEvent::HttpLogLevel sas_log_level) :
  _parent(parent),
  _deadline_ms(0L),
  _rand(1.0 / CONNECTION_AGE_MS),
  _fd(-1),
  _sas_log_level(sas_log_level)
{
}


/// Connection destructor
HttpConnectionLite::Connection::~Connection()
{
}


/// Is it time to recycle the connection? Expects CLOCK_MONOTONIC
/// current time, in milliseconds.
bool HttpConnectionLite::Connection::is_connection_expired(unsigned long now_ms)
{
  return (now_ms > _deadline_ms);
}


/// Update deadline to next appropriate value. Expects
/// CLOCK_MONOTONIC current time, in milliseconds.  Call on
/// successful connection.
void HttpConnectionLite::Connection::update_deadline(unsigned long now_ms)
{
  // Get the next desired inter-arrival time. Take a random sample
  // from an exponential distribution so as to avoid spikes.
  unsigned long interval_ms = (unsigned long)_rand();

  if ((_deadline_ms == 0L) ||
      ((_deadline_ms + interval_ms) < now_ms))
  {
    // This is the first request, or the new arrival time has
    // already passed (in which case things must be pretty
    // quiet). Just bump the next deadline into the future.
    _deadline_ms = now_ms + interval_ms;
  }
  else
  {
    // The new arrival time is yet to come. Schedule it relative to
    // the last intended time, so as not to skew the mean
    // upwards.

    // We don't recycle any connections in the UTs. (We could do this
    // by manipulating time, but would have no way of checking it
    // worked.)
    _deadline_ms += interval_ms; // LCOV_EXCL_LINE
  }
}


/// Set the remote IP, and update statistics.
void HttpConnectionLite::Connection::set_remote_ip(const std::string& value)  //< Remote IP, or "" if no connection.
{
  if (value == _stats_remote_ip)
  {
    return;
  }


  if (_parent->_stat_table != NULL)
  {
    update_snmp_ip_counts(value);
  }

  _stats_remote_ip = value;
}

void HttpConnectionLite::Connection::update_snmp_ip_counts(const std::string& value)  //< Remote IP, or "" if no connection.
{
  pthread_mutex_lock(&_parent->_lock);

  if (!_stats_remote_ip.empty())
  {
    if (_parent->_stat_table->get(_stats_remote_ip)->decrement() == 0)
    {
      _parent->_stat_table->remove(_stats_remote_ip);
    }
  }

  if (!value.empty())
  {
    _parent->_stat_table->get(value)->increment();
  }

  pthread_mutex_unlock(&_parent->_lock);
}

void HttpConnectionLite::cleanup_uuid(void *uuid_gen)
{
  delete (RandomUUIDGenerator*)uuid_gen; uuid_gen = NULL;
}

boost::uuids::uuid HttpConnectionLite::get_random_uuid()
{
  // Get the factory from thread local data (creating it if it doesn't exist).
  RandomUUIDGenerator* uuid_gen;
  uuid_gen =
    (RandomUUIDGenerator*)pthread_getspecific(_uuid_thread_local);

  if (uuid_gen == NULL)
  {
    uuid_gen = new RandomUUIDGenerator();
    pthread_setspecific(_uuid_thread_local, uuid_gen);
  }

  // _uuid_gen_ is a pointer to a callable object that returns a UUID.
  return (*uuid_gen)();
}

void HttpConnectionLite::Connection::sas_add_ip(SAS::Event& event, bool remote)
{
  bool ok;
  std::string ip;

  if (remote)
  {
    ok = get_remote_ip(ip);
  }
  else
  {
    ok = get_local_ip(ip);
  }

  if (ok)
  {
    event.add_var_param(ip);
  }
  else
  {
    event.add_var_param("unknown");
  }
}

void HttpConnectionLite::Connection::sas_add_port(SAS::Event& event, bool remote)
{
  bool ok;
  int port;

  if (remote)
  {
    ok = get_remote_port(port);
  }
  else
  {
    ok = get_local_port(port);
  }

  if (ok)
  {
    event.add_static_param(port);
  }
  else
  {
    event.add_static_param(0);
  }
}

void HttpConnectionLite::Connection::sas_add_ip_addrs_and_ports(SAS::Event& event)
{
  // Add the local IP and port.
  sas_add_ip(event, false);
  sas_add_port(event, false);

  // Now add the remote IP and port.
  sas_add_ip(event, true);
  sas_add_port(event, true);
}

void HttpConnectionLite::Connection::sas_log_http_req(SAS::TrailId trail,
                                                      const std::string& method_str,
                                                      const std::string& path,
                                                      const std::string& request_bytes,
                                                      uint32_t instance_id)
{
  if (_sas_log_level != SASEvent::HttpLogLevel::NONE)
  {
    int event_id = ((_sas_log_level == SASEvent::HttpLogLevel::PROTOCOL) ?
                    SASEvent::TX_HTTP_REQ : SASEvent::TX_HTTP_REQ_DETAIL);
    SAS::Event event(trail, event_id, instance_id);

    sas_add_ip_addrs_and_ports(event);
    event.add_compressed_param(request_bytes, &SASEvent::PROFILE_HTTP);
    event.add_var_param(method_str);
    event.add_var_param(Utils::url_unescape(path));

    SAS::report_event(event);
  }
}

void HttpConnectionLite::Connection::sas_log_http_rsp(SAS::TrailId trail,
                                                      long http_code,
                                                      const std::string& method_str,
                                                      const std::string& path,
                                                      const std::string& response_bytes,
                                                      uint32_t instance_id)
{
  if (_sas_log_level != SASEvent::HttpLogLevel::NONE)
  {
    int event_id = ((_sas_log_level == SASEvent::HttpLogLevel::PROTOCOL) ?
                    SASEvent::RX_HTTP_RSP : SASEvent::RX_HTTP_RSP_DETAIL);
    SAS::Event event(trail, event_id, instance_id);

    sas_add_ip_addrs_and_ports(event);

    event.add_static_param(http_code);
    event.add_compressed_param(response_bytes, &SASEvent::PROFILE_HTTP);
    event.add_var_param(method_str);
    event.add_var_param(Utils::url_unescape(path));

    SAS::report_event(event);
  }
}

void HttpConnectionLite::sas_log_http_abort(SAS::TrailId trail,
                                            HttpErrorResponseTypes reason,
                                            uint32_t instance_id)
{
  int event_id = ((_sas_log_level == SASEvent::HttpLogLevel::PROTOCOL) ?
                  SASEvent::HTTP_ABORT : SASEvent::HTTP_ABORT_DETAIL);
  SAS::Event event(trail, event_id, instance_id);
  event.add_static_param(static_cast<uint32_t>(reason));
  SAS::report_event(event);
}

#if 0
void HttpConnectionLite::sas_log_conn_error(SAS::TrailId trail,
                                            const char* remote_ip_addr,
                                            unsigned short remote_port,
                                            const std::string& method_str,
                                            const std::string& path,
                                            uint32_t instance_id)
{
  if (_sas_log_level != SASEvent::HttpLogLevel::NONE)
  {
    int event_id = ((_sas_log_level == SASEvent::HttpLogLevel::PROTOCOL) ?
                    SASEvent::HTTP_REQ_ERROR : SASEvent::HTTP_REQ_ERROR_DETAIL);
    SAS::Event event(trail, event_id, instance_id);

    event.add_static_param(remote_port);
    event.add_static_param(0);
    event.add_var_param(remote_ip_addr);
    event.add_var_param(method_str);
    event.add_var_param(Utils::url_unescape(path));
    event.add_var_param("Connection error");

    SAS::report_event(event);
  }
}
#endif

void HttpConnectionLite::host_port_from_server(const std::string& server, std::string& host, int& port)
{
  std::string server_copy = server;
  Utils::trim(server_copy);
  size_t colon_idx;
  if (((server_copy[0] != '[') ||
       (server_copy[server_copy.length() - 1] != ']')) &&
      ((colon_idx = server_copy.find_last_of(':')) != std::string::npos))
  {
    host = server_copy.substr(0, colon_idx);
    port = stoi(server_copy.substr(colon_idx + 1));
  }
  else
  {
    host = server_copy;
    port = 0;
  }
}

std::string HttpConnectionLite::host_from_server(const std::string& server)
{
  std::string host;
  int port;
  host_port_from_server(server, host, port);
  return host;
}

int HttpConnectionLite::port_from_server(const std::string& server)
{
  std::string host;
  int port;
  host_port_from_server(server, host, port);
  return port;
}

// Changes the underlying server used by this connection. Use this when
// the HTTPConnection was created without a server (e.g.
// ChronosInternalConnection)
void HttpConnectionLite::change_server(std::string override_server)
{
  _server = override_server;
  _host = host_from_server(override_server);
  _port = port_from_server(override_server);
}

// This function determines an appropriate absolute HTTP request timeout
// (in ms) given the target latency for requests that the downstream components
// will be using.
long HttpConnectionLite::calc_req_timeout_from_latency(int latency_us)
{
  return std::max(1, (latency_us * TIMEOUT_LATENCY_MULTIPLIER) / 1000);
}

bool HttpConnectionLite::Connection::
send_request_recv_response(const AddrInfo& ai,
                           bool recycle,
                           SAS::TrailId trail,
                           const std::string& method,
                           const std::string& path,
                           const std::vector<std::string>& request_headers,
                           const std::string& body,
                           HTTPCode& http_code,
                           std::map<std::string, std::string>* response_headers,
                           std::string* response_body)
{
  char buffer[16 * 1024];
  std::string request;
  std::string response;
  int rc;

  if (!establish_connection(ai, recycle))
  {
    return false;
  }

  if (!build_request_header(method, path, request_headers, body, request))
  {
    return false;
  }

  request.append(body);
  sas_log_http_req(trail, method, path, request, 0);

  send_all(request.c_str(), request.length());

  HttpResponseParser parser(&http_code, response_headers, response_body);

  do
  {
    rc = ::recv(_fd, buffer, sizeof(buffer), 0);
    TRC_DEBUG("Done a read of %d bytes", rc);

    if (rc < 0)
    {
      TRC_ERROR("Failed to receive");
      ::close(_fd); _fd = 1;
      return false;
    }
    else if (rc == 0)
    {
      TRC_ERROR("Connection closed");
      ::close(_fd); _fd = 1;
      return false;
    }

    int data_available = rc;

    if (parser.feed(buffer, data_available) < 0)
    {
      // Parser hit an error. Nothing we can do but close the connection.
      ::close(_fd); _fd = 1;
      return false;
    }

    response.append(buffer, data_available);
  }
  while (!parser.is_complete());

  TRC_DEBUG("Received %d bytes: %s", response.length(), response.c_str());
  sas_log_http_rsp(trail, 200, method, path, response, 0);

  return true;
}

bool HttpConnectionLite::Connection::establish_connection(const AddrInfo& ai,
                                                          bool recycle)
{
  if (!recycle && (_fd >= 0) && (ai == _remote_ai))
  {
    TRC_DEBUG("Keep using same connection");
    return true;
  }

  if (_fd >= 0)
  {
    TRC_DEBUG("Close existing connection");
    ::close(_fd); _fd = -1;
  }

  TRC_DEBUG("Establish a brand new connection");

  _fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (_fd < 0)
  {
    TRC_ERROR("Could not create socket: %d, %s", _fd, strerror(errno));
    return false;
  }

  int rc = 0;
  int enable = 1;
  rc = ::setsockopt(_fd, IPPROTO_TCP, TCP_NODELAY, (char *)&enable, sizeof(int));
  if (rc < 0)
  {
    TRC_ERROR("Error setting TCP_NODELAY: %d, %s", rc, strerror(errno));
    ::close(_fd); _fd = -1;
    return false;
  }

  sockaddr_in remote_sa_in = {0};
  remote_sa_in.sin_family = AF_INET;
  remote_sa_in.sin_addr = ai.address.addr.ipv4;
  remote_sa_in.sin_port = htons(ai.port);

  rc = ::connect(_fd, (sockaddr*)&remote_sa_in, sizeof(remote_sa_in));
  if (rc < 0)
  {
    TRC_ERROR("Failed to connect: %d, %s", rc, strerror(errno));
    ::close(_fd); _fd = -1;
    return false;
  }

  sockaddr_in local_sa_in = {0};
  socklen_t socklen = sizeof(local_sa_in);

  rc = getsockname(_fd, (sockaddr*)&local_sa_in, &socklen);
  if (rc < 0)
  {
    TRC_ERROR("Could not get local socket name");
    ::close(_fd); _fd = -1;
    return false;
  }

  _remote_ai = ai;

  _local_ai.transport = _remote_ai.transport;
  _local_ai.port = local_sa_in.sin_port;
  _local_ai.address.addr.ipv4 = local_sa_in.sin_addr;

  return true;
}

bool HttpConnectionLite::Connection::
build_request_header(const std::string& method,
                     const std::string& path,
                     const std::vector<std::string>& request_headers,
                     const std::string& body,
                     std::string& request)
{
  char buffer[16 * 1000];
  size_t len = 0;

  len += sprintf(buffer + len, "%s %s HTTP/1.1\r\n", method.c_str(), path.c_str());
  for(std::vector<std::string>::const_iterator hdr = request_headers.begin();
      hdr != request_headers.end();
      ++hdr)
  {
    len += sprintf(buffer + len, "%s\r\n", hdr->c_str());
  }

  if (!body.empty())
  {
    len += sprintf(buffer + len,
                      "Content-Length: %ld\r\n"
                      "Content-Type: application/json\r\n"
                      "\r\n",
                      body.length());
  }
  else
  {
    len += sprintf(buffer + len, "Content-Length: 0\r\n\r\n");
  }

  request.assign(buffer, len);
  return true;
}

bool HttpConnectionLite::Connection::send_all(const char* data, size_t len)
{
  size_t data_sent = 0;

  while (data_sent < len)
  {
    size_t rc = ::send(_fd, data + data_sent, (len - data_sent), 0);
    if (rc < 0)
    {
      TRC_ERROR("Failed to send");
      ::close(_fd); _fd = 1;
      return false;
    }
    else
    {
      TRC_DEBUG("Sent %d bytes: %.*s", rc, rc, data + data_sent);
      data_sent += rc;
    }
  }
  return true;
}

template<class T>
bool HttpConnectionLite::Connection::assign_if_connected(T& lhs, const T& rhs)
{
  if (_fd >= 0)
  {
    lhs = rhs;
    return true;
  }
  else
  {
    return false;
  }
}

bool HttpConnectionLite::Connection::get_local_ai(AddrInfo& ai)
{
  return assign_if_connected(ai, _local_ai);
}

bool HttpConnectionLite::Connection::get_remote_ai(AddrInfo& ai)
{
  return assign_if_connected(ai, _remote_ai);
}

bool HttpConnectionLite::Connection::get_local_ip(std::string& ip)
{
  return assign_if_connected(ip, _local_ai.address.to_string());
}

bool HttpConnectionLite::Connection::get_remote_ip(std::string& ip)
{
  return assign_if_connected(ip, _remote_ai.address.to_string());
}

bool HttpConnectionLite::Connection::get_local_port(int& port)
{
  return assign_if_connected(port, _remote_ai.port);
}

bool HttpConnectionLite::Connection::get_remote_port(int& port)
{
  return assign_if_connected(port, _local_ai.port);
}
