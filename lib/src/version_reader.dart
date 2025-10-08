import 'dart:io';

/// Reads the version from pubspec.yaml file
class VersionReader {
  static String? _cachedVersion;

  static String? getVersion() {
    if (_cachedVersion != null) {
      return _cachedVersion;
    }

    try {
      // Try to find pubspec.yaml in current directory or parent directories
      File? pubspecFile = _findPubspecFile();
      if (pubspecFile == null) {
        return null;
      }

      String content = pubspecFile.readAsStringSync();
      _cachedVersion = _parseVersionFromYaml(content);
      return _cachedVersion;
    } catch (e) {
      return null;
    }
  }

  /// Finds pubspec.yaml file starting from current directory
  static File? _findPubspecFile() {
    Directory current = Directory.current;

    // Check up to 5 parent directories
    for (int i = 0; i < 5; i++) {
      File pubspecFile = File('${current.path}/pubspec.yaml');
      if (pubspecFile.existsSync()) {
        return pubspecFile;
      }

      Directory parent = current.parent;
      if (parent.path == current.path) {
        // Reached root directory
        break;
      }
      current = parent;
    }

    return null;
  }

  /// Simple YAML parser for version field only
  static String? _parseVersionFromYaml(String yamlContent) {
    List<String> lines = yamlContent.split('\n');

    for (String line in lines) {
      String trimmed = line.trim();

      // Look for version: line
      if (trimmed.startsWith('version:')) {
        String versionPart = trimmed.substring(8).trim();

        // Remove quotes if present
        if (versionPart.startsWith('"') && versionPart.endsWith('"')) {
          versionPart = versionPart.substring(1, versionPart.length - 1);
        } else if (versionPart.startsWith("'") && versionPart.endsWith("'")) {
          versionPart = versionPart.substring(1, versionPart.length - 1);
        }

        return versionPart.isNotEmpty ? versionPart : null;
      }
    }

    return null;
  }

  static void clearCache() {
    _cachedVersion = null;
  }
}
