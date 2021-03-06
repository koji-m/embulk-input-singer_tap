# Singer Tap input plugin for Embulk

[![Gem Version](https://badge.fury.io/rb/embulk-input-singer_tap.svg)](https://badge.fury.io/rb/embulk-input-singer_tap)

This plugin runs a singer-tap and reads data from its stdout.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **tap_command**: singer-tap command name (string, required)
- **config**: singer-tap config file path (string, required`)
- **catalog**: singer-tap catalog file path (string, default:`null`)
- **properties**: singer-tap properties file path for legacy taps (string, default:`null`)
- **input_state**: singer-tap state file path (string, default:`null`)
- **output_state**: destination file path for STATE message (string, default:`null`)

## Example

```yaml
in:
  type: singer_tap
  tap_command: tap-github
  config: config.json
  properties: properties.json
  input_state: state.json
  output_state: state.json
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
