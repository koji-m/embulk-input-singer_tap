# Singer Tap input plugin for Embulk

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

## Example

```yaml
in:
  type: singer_tap
  tap_command: tap-github
  config: config.json
  properties: properties.json
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
