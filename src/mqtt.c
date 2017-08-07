/*
Copyright (c) 2014 Shin Hiroe

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

#include "mruby.h"
#include "mruby/array.h"
#include "mruby/class.h"
#include "mruby/data.h"
#include "mruby/numeric.h"
#include "mruby/string.h"
#include "mruby/variable.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTAsync.h"
#include "Thread.h"

extern void MQTTAsync_init();
extern void MQTTAsync_sleep(long milli_sec);

#define E_MQTT_ALREADY_CONNECTED_ERROR  (mrb_class_get(mrb, "MQTTAlreadyConnectedError"))
#define E_MQTT_NOT_CONNECTED_ERROR      (mrb_class_get(mrb, "MQTTNotConnectedError"))
#define E_MQTT_CONNECTION_FAILURE_ERROR (mrb_class_get(mrb, "MQTTConnectionFailureError"))
#define E_MQTT_SUBSCRIBE_ERROR          (mrb_class_get(mrb, "MQTTSubscribeFailureError"))
#define E_MQTT_PUBLISH_ERROR            (mrb_class_get(mrb, "MQTTPublishFailureError"))
#define E_MQTT_DISCONNECT_ERROR         (mrb_class_get(mrb, "MQTTDisconnectFailureError"))

typedef enum {
  MQTT_MESSAGE_ARRIVED,
  MQTT_DELIVERY_COMPLETE,
  MQTT_CONNECTION_LOST,
  MQTT_ON_CONNECT_SUCCESS,
  MQTT_ON_CONNECT_FAILURE,
  MQTT_ON_DISCONNECT_SUCCESS,
  MQTT_ON_DISCONNECT_FAILURE,
  MQTT_ON_PUBLISH_SUCCESS,
  MQTT_ON_PUBLISH_FAILURE,
  MQTT_ON_SUBSCRIBE_SUCCESS,
  MQTT_ON_SUBSCRIBE_FAILURE,
} mqtt_queue_type_tag;

typedef struct {
  mqtt_queue_type_tag tag;
  union {
    char *cause;
    MQTTAsync_token token;
    MQTTAsync_failureData failure;
    MQTTAsync_successData success;
    struct {
      char *topic;
      int topic_len;
      MQTTAsync_message *message;
    } message;
  } data;
} mqtt_queue_item;

typedef struct _mqtt_state {
  mrb_state *mrb;
  mrb_value self;
  MQTTAsync client;

  mutex_type queue_lock;
  mrb_int queue_size;
  mqtt_queue_item* queue_data;

  int token_count;
  int *tokens;
} mqtt_state;

// queue_lockでロックを必ずとること
static void
mqtt_push_to_queue(mqtt_state *m, mqtt_queue_item const *item)
{
  m->queue_data = realloc(m->queue_data, (size_t)sizeof(mqtt_queue_item) * (m->queue_size + 1));
  m->queue_data[m->queue_size++] = *item;
}

static void
copy_str(char **dst, char const *src)
{
  size_t len;
  if (src) {
    len = strlen(src) + 1;
    *dst = malloc(len);
    memcpy(*dst, src, len);
  } else {
    *dst = NULL;
  }
}

static void
copy_data(char **dst, char const *src, size_t len)
{
  *dst = malloc(len);
  memcpy(*dst, src, len);
}

static void
mqtt_unregister_token(mqtt_state *m, int const token)
{
  int i, token_found = FALSE;
  for (i = 0; i < m->token_count; ++i) {
    if (token_found) {
      m->tokens[i - 1] = m->tokens[i];
    } 
    else if (m->tokens[i] == token) {
      token_found = TRUE;
      m->token_count--;
    }
  }
}

/*******************************************************************
  MQTTMessage Class
 *******************************************************************/

// exp: message = MQTTMessage.new
static mrb_value
mqtt_msg_new(mrb_state *mrb)
{
  struct RClass *_class_message;
  _class_message = mrb_class_get(mrb, "MQTTMessage");
  return mrb_obj_new(mrb, _class_message, 0, NULL);
}

// exp: message.topic #=> "/temp/shimane"
static mrb_value
mqtt_msg_topic(mrb_state *mrb, mrb_value message)
{
  return mrb_iv_get(mrb, message, mrb_intern_lit(mrb, "topic"));
}

// exp: message.topic = "/tmp/shimane"
static mrb_value
mqtt_set_msg_topic(mrb_state *mrb, mrb_value message)
{
  mrb_value topic;
  mrb_get_args(mrb, "o", &topic);
  mrb_iv_set(mrb, message, mrb_intern_lit(mrb, "topic"), topic);
  return topic;
}

// exp: message.payload #=> "/temp/shimane"
static mrb_value
mqtt_msg_payload(mrb_state *mrb, mrb_value message)
{
  return mrb_iv_get(mrb, message, mrb_intern_lit(mrb, "payload"));
}

// exp: message.payload = "10"
static mrb_value
mqtt_set_msg_payload(mrb_state *mrb, mrb_value message)
{
  mrb_value payload;
  mrb_get_args(mrb, "o", &payload);
  mrb_iv_set(mrb, message, mrb_intern_lit(mrb, "payload"), payload);
  return payload;
}

/*******************************************************************
  MQTT Client Class Internal functions
 *******************************************************************/

static void
check_mqtt_connected(mrb_state *mrb, mqtt_state *m)
{
  if (m == NULL || !MQTTAsync_isConnected(m->client)) {
    mrb_raise(mrb, E_MQTT_NOT_CONNECTED_ERROR, "MQTT not connected");
  }
}

mrb_bool
clean_session_c(mrb_state* mrb, mrb_value self)
{
  return mrb_obj_eq(mrb, mrb_true_value(),
		    mrb_funcall(mrb, self, "clean_session", 0));
}

/*******************************************************************
  MQTT Call backs
 *******************************************************************/

static int
mqtt_msgarrvd(void *context, char *topicName, int topicLen,
	      MQTTAsync_message *message)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_MESSAGE_ARRIVED;
  item.data.message.topic = topicName;
  item.data.message.topic_len = topicLen;
  item.data.message.message = message;
  mqtt_push_to_queue(m, &item);
  Thread_unlock_mutex(m->queue_lock);
  return 1;
}

static void
mqtt_connlost(void *context, char *cause)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_CONNECTION_LOST;
  copy_str(&item.data.cause, cause);
  mqtt_push_to_queue(m, &item);
  Thread_unlock_mutex(m->queue_lock);
  return;
}

static void
mqtt_on_disconnect(void* context, MQTTAsync_successData* response)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_ON_DISCONNECT_SUCCESS;
  mrb_assert(!response);
  mqtt_push_to_queue(m, &item);
  // mqtt_unregister_token(m, response->token);
  Thread_unlock_mutex(m->queue_lock);
}

static void
mqtt_on_disconnect_failure(void* context, MQTTAsync_failureData* response)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_ON_DISCONNECT_FAILURE;
  item.data.failure = *response;
  copy_str(&item.data.failure.message, response->message);
  mqtt_push_to_queue(m, &item);
  // mqtt_unregister_token(m, response->token);
  Thread_unlock_mutex(m->queue_lock);
}

static void
mqtt_on_subscribe(void* context, MQTTAsync_successData* response)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_ON_SUBSCRIBE_SUCCESS;
  item.data.success = *response;
  mqtt_push_to_queue(m, &item);
  mqtt_unregister_token(m, response->token);
  Thread_unlock_mutex(m->queue_lock);
}

static void
mqtt_on_subscribe_failure(void* context, MQTTAsync_failureData* response)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;
  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_ON_SUBSCRIBE_FAILURE;
  item.data.failure = *response;
  copy_str(&item.data.failure.message, response->message);
  mqtt_push_to_queue(m, &item);
  mqtt_unregister_token(m, response->token);
  Thread_unlock_mutex(m->queue_lock);
}

static void
mqtt_on_publish(void* context, MQTTAsync_successData* response)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_ON_PUBLISH_SUCCESS;
  item.data.success = *response;
  copy_str(&item.data.success.alt.pub.destinationName, response->alt.pub.destinationName);
  copy_data(
      (char**)&item.data.success.alt.pub.message.payload,
      response->alt.pub.message.payload,
      response->alt.pub.message.payloadlen);
  mqtt_push_to_queue(m, &item);
  mqtt_unregister_token(m, response->token);
  Thread_unlock_mutex(m->queue_lock);
}

static void
mqtt_on_publish_failure(void* context, MQTTAsync_failureData* response)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_ON_PUBLISH_FAILURE;
  item.data.failure = *response;
  copy_str(&item.data.failure.message, response->message);
  mqtt_push_to_queue(m, &item);
  mqtt_unregister_token(m, response->token);
  Thread_unlock_mutex(m->queue_lock);
}

static void
mqtt_on_connect_failure(void* context, MQTTAsync_failureData* response)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_ON_CONNECT_FAILURE;
  item.data.failure = *response;
  copy_str(&item.data.failure.message, response->message);
  mqtt_push_to_queue(m, &item);
  // mqtt_unregister_token(m, response->token);
  Thread_unlock_mutex(m->queue_lock);
}

static void
mqtt_on_connect(void* context, MQTTAsync_successData* response)
{
  mqtt_state *m = (mqtt_state*)context;
  mqtt_queue_item item;

  Thread_lock_mutex(m->queue_lock);
  item.tag = MQTT_ON_CONNECT_SUCCESS;
  item.data.success = *response;
  copy_str(&item.data.success.alt.connect.serverURI, response->alt.connect.serverURI);
  mqtt_push_to_queue(m, &item);
  // mqtt_unregister_token(m, response->token);
  Thread_unlock_mutex(m->queue_lock);
}

/*******************************************************************
  MQTTClient Class
 *******************************************************************/

static mrb_value
mqtt_init(mrb_state *mrb, mrb_value self)
{
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "address"), mrb_nil_value());
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "client_id"), mrb_nil_value());
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "keep_alive"),
	     mrb_fixnum_value(20));
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "request_timeout"),
	     mrb_fixnum_value(30));
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "wait_interval"), mrb_float_value(mrb, 0.1));

  mrb_data_init(self, NULL, NULL);
  return self;
}

// exp: self.address  #=> "tcp://test.mosquitto.org:1883"
static mrb_value
mqtt_address(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "address"));
}

// exp: self.address = "tcp://test.mosquitto.org:1883"
static mrb_value
mqtt_set_address(mrb_state *mrb, mrb_value self)
{
  mrb_value address;
  mrb_get_args(mrb, "o", &address);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "address"), address);
  return address;
}

// exp: self.client_id  #=> "my_client_id"
static mrb_value
mqtt_client_id(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "client_id"));
}

// exp: self.client_id = "my-clilent-id"
static mrb_value
mqtt_set_client_id(mrb_state *mrb, mrb_value self)
{
  mrb_value client_id;
  mrb_get_args(mrb, "o", &client_id);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "client_id"), client_id);
  return client_id;
}

// exp: self.keep_alive #=> 20
static mrb_value
mqtt_keep_alive(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "keep_alive"));
}

// exp: self.keep_alive = 20
static mrb_value
mqtt_set_keep_alive(mrb_state *mrb, mrb_value self)
{
  mrb_value keep_alive;
  mrb_get_args(mrb, "o", &keep_alive);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "keep_alive"), keep_alive);
  return keep_alive;
}

// exp: self.trust_store #=> "path/to/trust_store"
static mrb_value
mqtt_trust_store(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "trust_store"));
}

// exp: self.trust_store = "path/to/trust_store"
static mrb_value
mqtt_set_trust_store(mrb_state *mrb, mrb_value self)
{
  mrb_value trust_store;
  mrb_get_args(mrb, "o", &trust_store);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "trust_store"), trust_store);
  return trust_store;
}

// exp: self.key_store #=> "path/to/key_store"
static mrb_value
mqtt_key_store(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "key_store"));
}

// exp: self.key_store = "path/to/key_store"
static mrb_value
mqtt_set_key_store(mrb_state *mrb, mrb_value self)
{
  mrb_value key_store;
  mrb_get_args(mrb, "o", &key_store);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "key_store"), key_store);
  return key_store;
}

// exp: self.private_key #=> "path/to/private_key"
static mrb_value
mqtt_private_key(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "private_key"));
}

// exp: self.private_key = "path/to/private_key"
static mrb_value
mqtt_set_private_key(mrb_state *mrb, mrb_value self)
{
  mrb_value private_key;
  mrb_get_args(mrb, "o", &private_key);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "private_key"), private_key);
  return private_key;
}

// exp: self.private_key_password #=> "password"
static mrb_value
mqtt_private_key_password(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "private_key_password"));
}

// exp: self.private_key_password = "password"
static mrb_value
mqtt_set_private_key_password(mrb_state *mrb, mrb_value self)
{
  mrb_value private_key_password;
  mrb_get_args(mrb, "o", &private_key_password);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "private_key_password"), private_key_password);
  return private_key_password;
}

// exp: self.request_timeout #=> 10
static mrb_value
mqtt_request_timeout(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "request_timeout"));
}

// exp: self.request_timeout = 60
static mrb_value
mqtt_set_request_timeout(mrb_state *mrb, mrb_value self)
{
  mrb_value timeout_sec;
  mrb_get_args(mrb, "o", &timeout_sec);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "request_timeout"), timeout_sec);
  return timeout_sec;
}

/*******************************************************************
  MQTT Client Class API
 *******************************************************************/

static void
mqtt_register_token(mqtt_state *m, MQTTAsync_responseOptions const *opts)
{
  Thread_lock_mutex(m->queue_lock);
  m->tokens = realloc(m->tokens, (size_t)sizeof(int) * (m->token_count + 1));
  m->tokens[m->token_count++] = opts->token;
  Thread_unlock_mutex(m->queue_lock);
}

static int
mqtt_connected(mrb_value self) {
  return DATA_PTR(self) && MQTTAsync_isConnected(((mqtt_state*)DATA_PTR(self))->client);
}

static mrb_value
mqtt_is_connected(mrb_state *mrb, mrb_value self)
{
  return mrb_bool_value(mqtt_connected(self));
}

static void
mqtt_client_free(mrb_state *mrb, void *p)
{
  mqtt_state *m = (mqtt_state*)p;

  if (!m) { return; }

  if (m->client) MQTTAsync_destroy(&m->client);
  free(m->queue_data);
  Thread_destroy_mutex(m->queue_lock);
  mrb_free(mrb, m);
}
static struct mrb_data_type mqtt_client_type = { "MQTTClient", mqtt_client_free };

// exp: self.connect  #=> true | false
static mrb_value
mqtt_connect(mrb_state *mrb, mrb_value self)
{
  mqtt_state *m;
  MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
  MQTTAsync_SSLOptions ssl_opts = MQTTAsync_SSLOptions_initializer;
  mrb_value m_trust_store,
            m_key_store, m_private_key, m_private_key_password;
  char *c_address, *c_client_id;
  mrb_int c_keep_alive;
  int rc;

  if (mqtt_connected(self)) {
    mrb_raise(mrb, E_MQTT_ALREADY_CONNECTED_ERROR, "MQTT Already connected");
  }

  m = mrb_malloc(mrb, sizeof(mqtt_state));
  m->mrb = mrb;
  m->self = self;
  m->queue_size = 0;
  m->queue_data = malloc(0);
  m->queue_lock = Thread_create_mutex();
  m->tokens = malloc(0);
  m->token_count = 0;
  mrb_data_init(self, m, &mqtt_client_type);

  m_trust_store = mqtt_trust_store(mrb, self);
  m_key_store = mqtt_key_store(mrb, self);
  m_private_key = mqtt_private_key(mrb, self);
  m_private_key_password = mqtt_private_key_password(mrb, self);

  c_keep_alive = (mrb_int)mrb_fixnum(mqtt_keep_alive(mrb, self));
  c_address = mrb_str_to_cstr(mrb, mqtt_address(mrb, self));
  c_client_id = mrb_str_to_cstr(mrb, mqtt_client_id(mrb, self));

  MQTTAsync_create(&m->client, c_address, c_client_id,
		   MQTTCLIENT_PERSISTENCE_NONE, NULL);

  MQTTAsync_setCallbacks(m->client, m, mqtt_connlost, mqtt_msgarrvd, NULL);

  conn_opts.keepAliveInterval = c_keep_alive;
  conn_opts.cleansession = clean_session_c(mrb, self);
  conn_opts.onSuccess = mqtt_on_connect;
  conn_opts.onFailure = mqtt_on_connect_failure;
  conn_opts.context = m;
  conn_opts.connectTimeout = mrb_fixnum(mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "request_timeout")));

  if (!mrb_nil_p(m_trust_store)) {
    char *c_trust_store = mrb_str_to_cstr(mrb, m_trust_store);
    char *c_key_store = mrb_str_to_cstr(mrb, m_key_store);
    char *c_private_key = mrb_str_to_cstr(mrb, m_private_key);
    char *c_private_key_password = mrb_str_to_cstr(mrb, m_private_key_password);
    ssl_opts.trustStore = c_trust_store;
    ssl_opts.keyStore = c_key_store;
    ssl_opts.privateKey = c_private_key;
    ssl_opts.privateKeyPassword = c_private_key_password;
    conn_opts.ssl = &ssl_opts;
  }

  if ((rc = MQTTAsync_connect(m->client, &conn_opts)) != MQTTASYNC_SUCCESS) {
    mrb_raise(mrb, E_MQTT_CONNECTION_FAILURE_ERROR, "connection failure");
  }

  return mrb_bool_value(TRUE);
}

static mrb_value
mqtt_disconnect(mrb_state *mrb, mrb_value self)
{
  mqtt_state *m = DATA_PTR(self);
  int rc;
  MQTTAsync_disconnectOptions opts = MQTTAsync_disconnectOptions_initializer;

  check_mqtt_connected(mrb, m);

  opts.onSuccess = mqtt_on_disconnect;
  opts.onFailure = mqtt_on_disconnect_failure;
  opts.context = m;
  opts.timeout = mrb_fixnum(mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "request_timeout")));

  if ((rc = MQTTAsync_disconnect(m->client, &opts)) != MQTTASYNC_SUCCESS) {
    mrb_raise(mrb, E_MQTT_DISCONNECT_ERROR, "disconnect failure");
  }

  return mrb_bool_value(TRUE);
}

static mrb_value
mqtt_publish(mrb_state *mrb, mrb_value self)
{
  mqtt_state *m = DATA_PTR(self);
  int rc;
  char *topic_p, *payload_p;
  mrb_int qos;
  mrb_bool retain;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  MQTTAsync_message pubmsg = MQTTAsync_message_initializer;

  check_mqtt_connected(mrb, m);

  mrb_get_args(mrb, "zzib", &topic_p, &payload_p, &qos, &retain);

  opts.onSuccess = mqtt_on_publish;
  opts.onFailure = mqtt_on_publish_failure;
  opts.context = m;

  pubmsg.payload = payload_p;
  pubmsg.payloadlen = strlen(payload_p);
  pubmsg.qos = qos;
  pubmsg.retained = retain;

  if ((rc = MQTTAsync_sendMessage(m->client, topic_p, &pubmsg, &opts)) != MQTTASYNC_SUCCESS) {
    mrb_raise(mrb, E_MQTT_PUBLISH_ERROR, "publish failure");
  }
  mqtt_register_token(m, &opts);

  return mrb_bool_value(TRUE);
}

static mrb_value
mqtt_subscribe(mrb_state *mrb, mrb_value self)
{
  mqtt_state *m = (mqtt_state*)DATA_PTR(self);
  int rc;
  mrb_int qos;
  char *topic_p;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;

  check_mqtt_connected(mrb, m);

  opts.onSuccess = mqtt_on_subscribe;
  opts.onFailure = mqtt_on_subscribe_failure;
  opts.context = m;

  mrb_get_args(mrb, "zi", &topic_p, &qos);

  if ((rc = MQTTAsync_subscribe(m->client, topic_p, qos, &opts)) != MQTTASYNC_SUCCESS) {
    mrb_raise(mrb, E_MQTT_SUBSCRIBE_ERROR, "subscribe failure");
  }
  mqtt_register_token(m, &opts);

  return mrb_bool_value(TRUE);
}

static mrb_value
mqtt_process_queue(mrb_state *mrb, mrb_value self)
{
  mqtt_state *m = (mqtt_state*)DATA_PTR(self);
  int i;
  mqtt_queue_item *data;
  int data_count;

  // swap queue
  Thread_lock_mutex(m->queue_lock);
  data = m->queue_data;
  data_count = m->queue_size;
  m->queue_data = malloc(0);
  m->queue_size = 0;
  Thread_unlock_mutex(m->queue_lock);

  for (i = 0; i < data_count; ++i) {
    mqtt_queue_item *item = data + i;
    mrb_value msg;
    switch(item->tag) {
      case MQTT_MESSAGE_ARRIVED:
        msg = mqtt_msg_new(mrb);
        mrb_funcall(mrb, msg, "topic=", 1, mrb_str_new(mrb, item->data.message.topic, item->data.message.topic_len));
        mrb_funcall(mrb, msg, "payload=", 1, mrb_str_new(mrb, item->data.message.message->payload, item->data.message.message->payloadlen));
        mrb_funcall(mrb, self, "on_message_callback", 1, msg);
        MQTTAsync_freeMessage(&item->data.message.message);
        MQTTAsync_free(item->data.message.topic);
        break;

      case MQTT_CONNECTION_LOST:
        mrb_funcall(mrb, self, "connlost_callback", 1, mrb_str_new_cstr(mrb, item->data.cause));
        free(item->data.cause);
        break;

      case MQTT_ON_CONNECT_SUCCESS:
        mrb_funcall(mrb, self, "on_connect_callback", 0);
        free(item->data.success.alt.connect.serverURI);
        break;

      case MQTT_ON_CONNECT_FAILURE:
        mrb_funcall(mrb, self, "on_connect_failure_callback", 0);
        free(item->data.failure.message);
        break;

      case MQTT_ON_DISCONNECT_SUCCESS:
        mrb_funcall(mrb, self, "on_disconnect_callback", 0);
        break;

      case MQTT_ON_DISCONNECT_FAILURE:
        mrb_funcall(mrb, self, "on_disconnect_failure_callback", 0);
        free(item->data.failure.message);
        break;

      case MQTT_ON_PUBLISH_SUCCESS:
        mrb_funcall(mrb, self, "on_publish_callback", 0);
        free(item->data.success.alt.pub.destinationName);
        free(item->data.success.alt.pub.message.payload);
        break;

      case MQTT_ON_PUBLISH_FAILURE:
        mrb_funcall(mrb, self, "on_publish_callback", 0);
        free(item->data.failure.message);
        break;

      case MQTT_ON_SUBSCRIBE_SUCCESS:
        mrb_funcall(mrb, self, "on_subscribe_callback", 0);
        break;

      case MQTT_ON_SUBSCRIBE_FAILURE:
        mrb_funcall(mrb, self, "on_subscribe_callback", 0);
        free(item->data.failure.message);
        break;

      // case MQTT_DELIVERY_COMPLETE:
      default: mrb_assert(FALSE);
    }
  }
  free(data);

  return self;
}

static mrb_value
mqtt_wait_for_completion(mrb_state *mrb, mrb_value self)
{
  mqtt_state *m = (mqtt_state*)DATA_PTR(self);
  mrb_value v_token;
  mrb_int token;
  mrb_float timeout;

  mrb_get_args(mrb, "of", &v_token, &timeout);
  if (mrb_nil_p(v_token)) {
    MQTTAsync_sleep(timeout * 1000);
    return mrb_nil_value();
  }

  mrb_get_args(mrb, "if", &token, &timeout);
  return mrb_bool_value(
      MQTTAsync_waitForCompletion(m->client, token, timeout * 1000) == MQTTASYNC_SUCCESS);
}

static mrb_value
mqtt_wait_interval(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, mrb_intern_lit(mrb, "wait_interval"));
}
static mrb_value
mqtt_set_wait_interval(mrb_state *mrb, mrb_value self)
{
  mrb_float v;
  mrb_get_args(mrb, "f", &v);
  mrb_iv_set(mrb, self, mrb_intern_lit(mrb, "wait_interval"), mrb_float_value(mrb, v));
  return mrb_float_value(mrb, v);
}

static mrb_value
mqtt_tokens(mrb_state *mrb, mrb_value self)
{
  mqtt_state *m = (mqtt_state*)DATA_PTR(self);
  mrb_value ary;
  int i;

  Thread_lock_mutex(m->queue_lock);

  ary = mrb_ary_new_capa(mrb, m->token_count);

  for (i = 0; i < m->token_count; ++i) {
    mrb_ary_push(mrb, ary, mrb_fixnum_value(m->tokens[i]));
  }

  Thread_unlock_mutex(m->queue_lock);

  return ary;
}

static mrb_bool mqtt_initialized = FALSE;

void
mrb_mruby_mqtt_gem_init(mrb_state* mrb)
{
  struct RClass *d, *c;

  if (!mqtt_initialized) {
    mqtt_initialized = TRUE;
    MQTTAsync_init();
  }

  mrb_define_class(mrb, "MQTTConnectionFailureError", mrb->eStandardError_class);
  mrb_define_class(mrb, "MQTTSubscribeFailureError",  mrb->eStandardError_class);
  mrb_define_class(mrb, "MQTTPublishFailureError",    mrb->eStandardError_class);
  mrb_define_class(mrb, "MQTTDisconnectFailureError", mrb->eStandardError_class);
  mrb_define_class(mrb, "MQTTNullClientError",        mrb->eStandardError_class);
  mrb_define_class(mrb, "MQTTAlreadyConnectedError",  mrb->eStandardError_class);
  mrb_define_class(mrb, "MQTTNotConnectedError",  mrb->eStandardError_class);

  d = mrb_define_class(mrb, "MQTTMessage", mrb->object_class);
  MRB_SET_INSTANCE_TT(d, MRB_TT_DATA);
  mrb_define_method(mrb, d, "topic", mqtt_msg_topic, MRB_ARGS_NONE());
  mrb_define_method(mrb, d, "topic=", mqtt_set_msg_topic, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, d, "payload", mqtt_msg_payload, MRB_ARGS_NONE());
  mrb_define_method(mrb, d, "payload=", mqtt_set_msg_payload, MRB_ARGS_REQ(1));

  c = mrb_define_class(mrb, "MQTTClient", mrb->object_class);
  MRB_SET_INSTANCE_TT(c, MRB_TT_DATA);

  mrb_define_method(mrb, c, "initialize", mqtt_init, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "address", mqtt_address, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "address=", mqtt_set_address, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, c, "client_id", mqtt_client_id, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "client_id=", mqtt_set_client_id, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, c, "keep_alive", mqtt_keep_alive, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "keep_alive=", mqtt_set_keep_alive, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, c, "trust_store", mqtt_trust_store, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "trust_store=", mqtt_set_trust_store, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, c, "key_store", mqtt_key_store, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "key_store=", mqtt_set_key_store, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, c, "private_key", mqtt_private_key, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "private_key=", mqtt_set_private_key, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, c, "private_key_password", mqtt_private_key_password, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "private_key_password=", mqtt_set_private_key_password, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, c, "request_timeout", mqtt_request_timeout, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "request_timeout=", mqtt_set_request_timeout, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "_connect", mqtt_connect, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "connected?", mqtt_is_connected, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "publish_internal", mqtt_publish, MRB_ARGS_REQ(4));
  mrb_define_method(mrb, c, "subscribe_internal", mqtt_subscribe, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, c, "_disconnect", mqtt_disconnect, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "tokens", mqtt_tokens, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "wait_interval", mqtt_wait_interval, MRB_ARGS_NONE());
  mrb_define_method(mrb, c, "wait_interval=", mqtt_set_wait_interval, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, c, "wait_for_completion", mqtt_wait_for_completion, MRB_ARGS_REQ(2));
  mrb_define_method(mrb, c, "process_queue", mqtt_process_queue, MRB_ARGS_NONE());
}

void
mrb_mruby_mqtt_gem_final(mrb_state* mrb)
{
}
