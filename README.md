#mruby-mqtt

MQTT protocol library.

MQTT is a lightweight M2M,IoT protocol publish/subscribe messaging.
The mruby-mqtt is implemented as wrapper of [Paho C library](http://www.eclipse.org/paho/)

##Installing

Write in /mruby/build_config.rb

```ruby
MRuby::Build.new do |conf|

  conf.gem :github => 'hiroeorz/mruby-mqtt', :branch => 'master'

  conf.linker do |linker|
    #linker.link_options = "%{flags} -o %{outfile} %{objs} %{libs}"
    linker.link_options = "%{flags} -o %{outfile} %{objs} %{libs} -lpthread -Wl -lm"
  end

end
```

##Examples

###Setup

```ruby
MQTTClient.connect("tcp://test.mosquitto.org:1883", "mruby") do |c|
  c.on_connect   = -> { c.subscribe("/temp/shimane", 0)}
  c.on_subscribe = -> { puts "subscribe success"}
  c.on_publish   = -> { puts "publish success"}
end
```

callbacks

- <code>on_connect = -> { ... }</code>
- <code>on_subscribe = -> { ... }</code>
- <code>on_publish = -> { ... }</code>
- <code>on_disconnect = -> { ... }</code>
- <code>on_connect_failure = -> { ... }</code>
- <code>on_subscribe_failure = -> { ... }</code>
- <code>on_connlost = -> { ... }</code>
- <code>on_message = -> { |message| ... }</code>

on_message callback receive one argument, that is instance of MQTTMessage.

```ruby
c.on_message = -> { |message|
  puts message.topic
  puts message.payload
}
```

###Publish.

```ruby
mqtt = MQTTClient.instance
mqtt.publish("/mytopic", "mydata", 1)
```

###Disconnect

```ruby
mqtt = MQTTClient.instance
mqtt.on_disconnect = -> { puts "disconnected." }
mqtt.disconnect
```

##Limitations

Only one MQTTClient instance created per one os process. This means that only one connection created to the broker per os process.

##License
See source code files.
