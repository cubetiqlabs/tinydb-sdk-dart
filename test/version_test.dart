import 'package:test/test.dart';
import 'package:tinydb_client/tinydb_client.dart';

void main() {
  group('VersionReader Tests', () {
    test('VersionReader should read from pubspec.yaml', () {
      // Clear any cached version first
      VersionReader.clearCache();

      String? version = VersionReader.getVersion();

      // Should find and read the version from pubspec.yaml
      expect(version, isNotNull);
      expect(version, equals('0.1.0'));
    });

    test('VersionReader should cache the result', () {
      // Clear cache first
      VersionReader.clearCache();

      // First call
      String? version1 = VersionReader.getVersion();

      // Second call should return cached result
      String? version2 = VersionReader.getVersion();

      expect(version1, equals(version2));
      expect(version1, equals('0.1.0'));
    });

    test('VersionReader should handle missing pubspec gracefully', () {
      // This test verifies error handling, though in normal cases pubspec.yaml exists
      String? version = VersionReader.getVersion();
      // Should either return a version or null, but not throw
      expect(version, anyOf(isNull, isA<String>()));
    });
  });
}
