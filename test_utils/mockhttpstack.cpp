/**
 * @file mockhttpstack.cpp Mock HTTP stack.
 *
 * Project Clearwater - IMS in the cloud.
 * Copyright (C) 2014  Metaswitch Networks Ltd
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

extern "C" {
#include <evhtp.h>
}

#include <mockhttpstack.hpp>

evhtp_request_t*
MockHttpStack::Request::fake_evhtp_request_new(std::string path,
                                               std::string file,
                                               std::string query)
{
  evhtp_request_t* req = evhtp_request_new(NULL, NULL);
  req->conn = (evhtp_connection_t*)calloc(sizeof(evhtp_connection_t), 1);
  req->conn->parser = htparser_new();
  htparser_init(req->conn->parser, htp_type_request);
  req->uri = (evhtp_uri_t*)calloc(sizeof(evhtp_uri_t), 1);
  req->uri->path = (evhtp_path_t*)calloc(sizeof(evhtp_path_t), 1);
  req->uri->path->full = strdup((path + file).c_str());
  req->uri->path->file = strdup(file.c_str());
  req->uri->path->path = strdup(path.c_str());
  req->uri->path->match_start = (char*)calloc(1, 1);
  req->uri->path->match_end = (char*)calloc(1, 1);
  req->uri->query = evhtp_parse_query(query.c_str(), query.length());
  return req;
}

void MockHttpStack::Request::fake_evhtp_request_delete(evhtp_request_t* req)
{
  free(req->conn->parser);
  free(req->conn);
  evhtp_request_free(req);
}
