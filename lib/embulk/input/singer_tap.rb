Embulk::JavaPlugin.register_input(
  "singer_tap", "org.embulk.input.singer_tap.SingerTapInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
