/**
 * @file load_monitor.cpp LoadMonitor class methods.
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

#include "load_monitor.h"
#include "log.h"
#include "snmp_continuous_accumulator_table.h"
#include "snmp_scalar.h"
#include "sasevent.h"

TokenBucket::TokenBucket(int s, float r)
{
  max_size = s;
  tokens = max_size;
  rate = r;
  clock_gettime(CLOCK_MONOTONIC, &replenish_time);
}

bool TokenBucket::get_token()
{
  replenish_bucket();
  bool rc = (tokens >= 1);

  if (rc)
  {
    tokens -= 1;
  }

  return rc;
}

void TokenBucket::update_rate(float new_rate)
{
  rate = new_rate;
}

void TokenBucket::replenish_bucket()
{
  timespec new_replenish_time;
  clock_gettime(CLOCK_MONOTONIC, &new_replenish_time);
  float timediff = (new_replenish_time.tv_nsec - replenish_time.tv_nsec) / 1000.0 +
                   (new_replenish_time.tv_sec - replenish_time.tv_sec) * 1000000.0;
  // The rate is in tokens/sec, and the timediff is in usec.
  tokens += ((rate * timediff) / 1000000.0);
  replenish_time = new_replenish_time;

  if (tokens >= max_size)
  {
    tokens = max_size;
  }
}

LoadMonitor::LoadMonitor(int init_target_latency, int max_bucket_size,
                         float init_token_rate, float init_min_token_rate,
                         SNMP::ContinuousAccumulatorTable* token_rate_table,
                         SNMP::U32Scalar* smoothed_latency_scalar,
                         SNMP::U32Scalar* target_latency_scalar,
                         SNMP::U32Scalar* penalties_scalar,
                         SNMP::U32Scalar* token_rate_scalar)
                         : bucket(max_bucket_size, init_token_rate),
                           _token_rate_table(token_rate_table),
                           _smoothed_latency_scalar(smoothed_latency_scalar),
                           _target_latency_scalar(target_latency_scalar),
                           _penalties_scalar(penalties_scalar),
                           _token_rate_scalar(token_rate_scalar)
{
  pthread_mutexattr_t attrs;
  pthread_mutexattr_init(&attrs);
  pthread_mutexattr_settype(&attrs, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&_lock, &attrs);
  pthread_mutexattr_destroy(&attrs);

  TRC_STATUS("Constructing LoadMonitor");
  TRC_STATUS("   Target latency (usecs)   : %d", init_target_latency);
  TRC_STATUS("   Max bucket size          : %d", max_bucket_size);
  TRC_STATUS("   Initial token fill rate/s: %f", init_token_rate);
  TRC_STATUS("   Min token fill rate/s    : %f", init_min_token_rate);

  REQUESTS_BEFORE_ADJUSTMENT = 20;
  SECONDS_BEFORE_ADJUSTMENT = 2;

  // Adjustment parameters for token bucket
  DECREASE_THRESHOLD = 0.0;
  DECREASE_FACTOR = 1.2;
  INCREASE_THRESHOLD = -0.005;
  INCREASE_FACTOR = 0.5;

  accepted = 0;
  rejected = 0;
  penalties = 0;
  pending_count = 0;
  max_pending_count = 0;
  target_latency = init_target_latency;
  smoothed_latency = init_target_latency;
  adjust_count = 0;

  timespec current_time;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &current_time);
  last_adjustment_time_ms = (current_time.tv_sec * 1000) + (current_time.tv_nsec / 1000000);
  min_token_rate = init_min_token_rate;

  // As this statistics reporting is continuous, we should
  // publish the statistics when initialised.
  update_statistics();
}

LoadMonitor::~LoadMonitor()
{
  // Destroy the lock
  pthread_mutex_destroy(&_lock);
}

bool LoadMonitor::admit_request(SAS::TrailId trail)
{
  pthread_mutex_lock(&_lock);

  if (bucket.get_token())
  {
    SAS::Event event(trail, SASEvent::LOAD_MONITOR_ACCEPTED_REQUEST, trail);
    event.add_static_param(bucket.rate);
    event.add_static_param(bucket.token_count());
    SAS::report_event(event);

    // Got a token from the bucket, so admit the request
    accepted += 1;
    pending_count += 1;

    if (pending_count > max_pending_count)
    {
      max_pending_count = pending_count;
    }

    pthread_mutex_unlock(&_lock);
    return true;
  }
  else
  {
    float accepted_percent = (accepted + rejected == 0) ?
                             100.0 :
                             100 * (((float) accepted) / (accepted + rejected));
    timespec current_time;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &current_time);
    unsigned long time_passed_ms = ((current_time.tv_sec * 1000) +
                                    (current_time.tv_nsec / 1000000)) -
                                   last_adjustment_time_ms;

    SAS::Event event(trail, SASEvent::LOAD_MONITOR_REJECTED_REQUEST, trail);
    event.add_static_param(bucket.rate);
    event.add_static_param(accepted_percent);
    event.add_static_param(time_passed_ms);
    SAS::report_event(event);

    rejected += 1;
    pthread_mutex_unlock(&_lock);
    return false;
  }
}

void LoadMonitor::incr_penalties()
{
  pthread_mutex_lock(&_lock);
  penalties += 1;
  pthread_mutex_unlock(&_lock);
}


int LoadMonitor::get_target_latency_us()
{
  return target_latency;
}

void LoadMonitor::request_complete(int latency)
{
  pthread_mutex_lock(&_lock);
  pending_count -= 1;
  smoothed_latency = (7 * smoothed_latency + latency) / 8;
  adjust_count += 1;

  if (adjust_count >= REQUESTS_BEFORE_ADJUSTMENT)
  {
    // We've seen the right number of requests, but ensure
    // that an appropriate amount of time has passed, so the rate doesn't
    // fluctuate wildly if latency spikes for a few milliseconds
    timespec current_time;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &current_time);
    unsigned long current_time_ms = (current_time.tv_sec * 1000) + (current_time.tv_nsec / 1000000);
    if (current_time_ms >= (last_adjustment_time_ms + (SECONDS_BEFORE_ADJUSTMENT * 1000)))
    {
      // This algorithm is based on the Welsh and Culler "Adaptive Overload
      // Control for Busy Internet Servers" paper, although based on a smoothed
      // mean latency, rather than the 90th percentile as per the paper.
      // Also, the additive increase is scaled as a proportion of the maximum
      // bucket size, rather than an absolute number as per the paper.
      float err = ((float) (smoothed_latency - target_latency)) / target_latency;

      // Work out the percentage of accepted requests (for logs)
      float accepted_percent = (accepted + rejected == 0) ? 100.0 : 100 * (((float) accepted) / (accepted + rejected));

      TRC_INFO("Accepted %f%% of requests, latency error = %f, overload responses = %d",
          accepted_percent, err, penalties);

      // latency is above where we want it to be, or we are getting overload responses from
      // Homer/Homestead, so adjust the rate downwards by a multiplicative factor

      if (err > DECREASE_THRESHOLD || penalties > 0)
      {
        float new_rate = bucket.rate / DECREASE_FACTOR;
        if (new_rate < min_token_rate)
        {
          new_rate = min_token_rate;
        }
        bucket.update_rate(new_rate);
        TRC_STATUS("Maximum incoming request rate/second decreased to %f "
                   "(based on a smoothed mean latency of %d and %d upstream overload responses)",
                   bucket.rate,
                   smoothed_latency,
                   penalties);
      }
      else if (err < INCREASE_THRESHOLD)
      {
        // Our latency is below the threshold, so increasing our permitted request rate would be
        // sensible. Before doing that, we check that we're using a significant proportion of our
        // current rate - if we're allowing 100 requests/sec, and we get 1 request/sec because it's
        // a quiet period, then it's going to be handled quickly, but that's not sufficient evidence
        // to increase our rate.
        int ms_passed = (current_time_ms - last_adjustment_time_ms);
        float maximum_permitted_requests = bucket.rate * ms_passed / 1000;

        // Arbitrary threshold - require 50% of our current permitted rate to be used
        float minimum_threshold = maximum_permitted_requests * 0.5;

        if (accepted > minimum_threshold)
        {
          float new_rate = bucket.rate + (-1 * err * bucket.max_size * INCREASE_FACTOR);
          bucket.update_rate(new_rate);
          TRC_STATUS("Maximum incoming request rate/second increased to %f "
                     "(based on a smoothed mean latency of %d, %d upstream "
                     "overload responses, %dms time passing, %d accepted "
                     "requests, and %d rejected requests).",
                     bucket.rate,
                     smoothed_latency,
                     penalties,
                     ms_passed,
                     accepted,
                     rejected);
        }
        else
        {
          TRC_STATUS("Maximum incoming request rate/second unchanged - only handled %d requests"
                     " in last %dms, minimum threshold for a change is %f",
                     accepted,
                     ms_passed,
                     minimum_threshold);
        }
      }
      else
      {
        TRC_DEBUG("Maximum incoming request rate/second is unchanged at %f",
                  bucket.rate);
      }

      update_statistics();

      // Reset counts
      last_adjustment_time_ms = current_time_ms;
      adjust_count = 0;
      accepted = 0;
      rejected = 0;
      penalties = 0;
    }
  }

  pthread_mutex_unlock(&_lock);
}

void LoadMonitor::update_statistics()
{
  if (_smoothed_latency_scalar != NULL)
  {
    _smoothed_latency_scalar->value = smoothed_latency;
  }
  if (_target_latency_scalar != NULL)
  {
    _target_latency_scalar->value = target_latency;
  }
  if (_penalties_scalar != NULL)
  {
    _penalties_scalar->value = penalties;
  }
  if (_token_rate_table != NULL)
  {
    _token_rate_table->accumulate(bucket.rate);
  }
  if (_token_rate_scalar != NULL)
  {
    _token_rate_scalar->value = bucket.rate;
  }
}
