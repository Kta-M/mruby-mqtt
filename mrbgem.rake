MRuby::Gem::Specification.new('mruby-mqtt') do |spec|
  spec.license = 'MIT'
  spec.author = 'hiroe.orz@gmail.com'
  spec.summary = 'MQTT Client class'

  add_dependency 'mruby-singleton'
  add_dependency 'mruby-time', core: 'mruby-time'
end
