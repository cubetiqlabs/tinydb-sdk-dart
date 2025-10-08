import 'dart:async';
import 'dart:convert';

import 'package:collection/collection.dart';
import 'package:http/http.dart' as http;
import 'package:meta/meta.dart';
import 'package:tinydb_client/tinydb_client.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

typedef WebSocketConnector = WebSocketChannel Function(
  Uri uri, {
  Iterable<String>? protocols,
  Map<String, dynamic>? headers,
  Duration? pingInterval,
});

WebSocketChannel _defaultWebSocketConnector(
  Uri uri, {
  Iterable<String>? protocols,
  Map<String, dynamic>? headers,
  Duration? pingInterval,
}) {
  return IOWebSocketChannel.connect(
    uri,
    protocols: protocols,
    headers: headers,
    pingInterval: pingInterval,
  );
}

typedef JsonMap = Map<String, dynamic>;

enum FieldType {
  string,
  number,
  boolean,
  uuid,
  date,
  datetime,
  object,
  array,
}

extension FieldTypeValue on FieldType {
  String get value => name;
}

enum PrimaryKeyType { uuid, number, string }

extension PrimaryKeyTypeValue on PrimaryKeyType {
  String get value => name;
}

@immutable
class FieldDefinition {
  final FieldType type;
  final bool required;
  final bool allowNull;
  final String? description;
  final List<String>? enumeration;
  final FieldDefinition? items;

  const FieldDefinition({
    required this.type,
    this.required = false,
    this.allowNull = false,
    this.description,
    this.enumeration,
    this.items,
  });

  const FieldDefinition._internal({
    required this.type,
    this.required = false,
    this.allowNull = false,
    this.description,
    this.enumeration,
    this.items,
  });

  factory FieldDefinition.string({
    bool required = false,
    bool allowNull = false,
    String? description,
    List<String>? values,
  }) =>
      FieldDefinition._internal(
        type: FieldType.string,
        required: required,
        allowNull: allowNull,
        description: description,
        enumeration: values,
      );

  factory FieldDefinition.number({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) =>
      FieldDefinition._internal(
        type: FieldType.number,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.boolean({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) =>
      FieldDefinition._internal(
        type: FieldType.boolean,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.uuid({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) =>
      FieldDefinition._internal(
        type: FieldType.uuid,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.date({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) =>
      FieldDefinition._internal(
        type: FieldType.date,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.datetime({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) =>
      FieldDefinition._internal(
        type: FieldType.datetime,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.object({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) =>
      FieldDefinition._internal(
        type: FieldType.object,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.array({
    bool required = false,
    bool allowNull = false,
    String? description,
    FieldDefinition? items,
  }) =>
      FieldDefinition._internal(
        type: FieldType.array,
        required: required,
        allowNull: allowNull,
        description: description,
        items: items,
      );

  JsonMap toJson() {
    final map = <String, dynamic>{
      'type': type.value,
    };
    if (required) map['required'] = true;
    if (allowNull) map['allowNull'] = true;
    if (description != null) map['description'] = description;
    if (enumeration != null) map['enum'] = enumeration;
    if (items != null) map['items'] = items!.toJson();
    return map;
  }
}

@immutable
class CollectionSchemaDefinition {
  final Map<String, FieldDefinition> fields;
  final String? description;

  const CollectionSchemaDefinition({
    required this.fields,
    this.description,
  });

  JsonMap toJson() => {
        'fields': fields.map((key, value) => MapEntry(key, value.toJson())),
        if (description != null) 'description': description,
      };
}

@immutable
class PrimaryKeyConfig {
  final String? field;
  final PrimaryKeyType? type;
  final bool? auto;

  const PrimaryKeyConfig({this.field, this.type, this.auto});

  JsonMap toJson() => {
        if (field != null) 'field': field,
        if (type != null) 'type': type!.value,
        if (auto != null) 'auto': auto,
      };
}

@immutable
class Pagination {
  final int? limit;
  final int? offset;
  final int? count;
  final String? nextCursor;
  final bool? hasMore;

  const Pagination({
    this.limit,
    this.offset,
    this.count,
    this.nextCursor,
    this.hasMore,
  });

  factory Pagination.fromJson(dynamic json) {
    if (json is! Map<String, dynamic>) {
      return const Pagination();
    }
    return Pagination(
      limit: json['limit'] as int?,
      offset: json['offset'] as int?,
      count: json['count'] as int?,
      nextCursor: json['next_cursor'] as String?,
      hasMore: json['has_more'] as bool?,
    );
  }
}

@immutable
class ListResult<T extends Map<String, dynamic>> {
  final List<DocumentRecord<T>> items;
  final Pagination pagination;

  const ListResult({required this.items, required this.pagination});
}

@immutable
class QueryResult<T extends Map<String, dynamic>> {
  final List<DocumentRecord<T>> items;
  final Pagination pagination;

  const QueryResult({required this.items, required this.pagination});
}

@immutable
class AggregatedQueryResult<T extends Map<String, dynamic>> {
  final List<DocumentRecord<T>> items;
  final int pageCount;
  final bool exhausted;
  final Pagination lastPagination;

  const AggregatedQueryResult({
    required this.items,
    required this.pageCount,
    required this.exhausted,
    required this.lastPagination,
  });

  String? get nextCursor => lastPagination.nextCursor;
}

@immutable
class QueryProgress {
  final int pageCount;
  final int itemCount;
  final Pagination lastPagination;
  final bool done;
  final int? maxPages;
  final int? maxItems;

  const QueryProgress({
    required this.pageCount,
    required this.itemCount,
    required this.lastPagination,
    required this.done,
    this.maxPages,
    this.maxItems,
  });

  String? get nextCursor => lastPagination.nextCursor;
  bool get hasNextCursor => nextCursor != null && nextCursor!.isNotEmpty;
}

enum CollectionWatchEventKind {
  initial,
  ack,
  create,
  update,
  delete,
  keepalive,
  error,
}

@immutable
class CollectionWatchEvent<T extends Map<String, dynamic>> {
  final CollectionWatchEventKind kind;
  final String? documentId;
  final T? data;
  final DocumentRecord<T>? document;
  final DateTime? timestamp;
  final Map<String, dynamic>? raw;

  const CollectionWatchEvent({
    required this.kind,
    this.documentId,
    this.data,
    this.document,
    this.timestamp,
    this.raw,
  });

  factory CollectionWatchEvent.initial(DocumentRecord<T> document) {
    return CollectionWatchEvent<T>(
      kind: CollectionWatchEventKind.initial,
      documentId: document.id,
      data: document.data,
      document: document,
      timestamp: DateTime.tryParse(document.updatedAt) ??
          DateTime.tryParse(document.createdAt),
      raw: null,
    );
  }

  factory CollectionWatchEvent.ack(Map<String, dynamic> payload) {
    return CollectionWatchEvent<T>(
      kind: CollectionWatchEventKind.ack,
      raw: Map<String, dynamic>.from(payload),
    );
  }

  factory CollectionWatchEvent.change({
    required CollectionWatchEventKind kind,
    String? documentId,
    T? data,
    DocumentRecord<T>? document,
    DateTime? timestamp,
    Map<String, dynamic>? raw,
  }) {
    return CollectionWatchEvent<T>(
      kind: kind,
      documentId: documentId,
      data: data,
      document: document,
      timestamp: timestamp,
      raw: raw != null ? Map<String, dynamic>.from(raw) : null,
    );
  }

  factory CollectionWatchEvent.keepalive(Map<String, dynamic>? payload) {
    return CollectionWatchEvent<T>(
      kind: CollectionWatchEventKind.keepalive,
      raw: payload != null ? Map<String, dynamic>.from(payload) : null,
    );
  }

  factory CollectionWatchEvent.error(Object error,
      {Map<String, dynamic>? raw}) {
    return CollectionWatchEvent<T>(
      kind: CollectionWatchEventKind.error,
      raw: {
        'error': error.toString(),
        if (raw != null) 'payload': Map<String, dynamic>.from(raw),
      },
    );
  }
}

abstract class CancellationToken {
  bool get isCancelled;
  Future<void> get whenCancelled;

  const CancellationToken();

  static const CancellationToken none = _NoneCancellationToken();

  factory CancellationToken.fromFuture(Future<void> future) {
    return _FutureCancellationToken(future);
  }
}

class CancellationTokenSource {
  CancellationTokenSource();

  final Completer<void> _completer = Completer<void>();
  bool _cancelled = false;

  CancellationToken get token =>
      _SourceCancellationToken(_completer.future, () => _cancelled);

  bool get isCancelled => _cancelled;

  void cancel() {
    if (_cancelled) {
      return;
    }
    _cancelled = true;
    _completer.complete();
  }
}

class _NoneCancellationToken implements CancellationToken {
  const _NoneCancellationToken();

  static final Future<void> _never = Completer<void>().future;

  @override
  bool get isCancelled => false;

  @override
  Future<void> get whenCancelled => _never;
}

class _FutureCancellationToken implements CancellationToken {
  _FutureCancellationToken(Future<void> future)
      : whenCancelled = future,
        _isCancelled = false {
    future.catchError((_) {});
    future.whenComplete(() {
      _isCancelled = true;
    });
  }

  @override
  bool get isCancelled => _isCancelled;

  @override
  final Future<void> whenCancelled;

  bool _isCancelled;
}

class _SourceCancellationToken implements CancellationToken {
  _SourceCancellationToken(this._future, this._isCancelled);

  final Future<void> _future;
  final bool Function() _isCancelled;

  @override
  bool get isCancelled => _isCancelled();

  @override
  Future<void> get whenCancelled => _future;
}

@immutable
class SyncParams {
  final DateTime? since;
  final String? cursor;
  final int? limit;
  final bool? includeDeleted;

  const SyncParams({this.since, this.cursor, this.limit, this.includeDeleted});

  Map<String, String> toQuery() {
    final map = <String, String>{};
    if (since != null) map['since'] = since!.toUtc().toIso8601String();
    if (cursor != null) map['cursor'] = cursor!;
    if (limit != null) map['limit'] = '$limit';
    if (includeDeleted != null) {
      map['include_deleted'] = includeDeleted! ? 'true' : 'false';
    }
    return map;
  }
}

@immutable
class SyncChange<T extends Map<String, dynamic>> {
  final String changeType;
  final DocumentRecord<T> document;

  const SyncChange({required this.changeType, required this.document});
}

@immutable
class SyncResult<T extends Map<String, dynamic>> {
  final List<SyncChange<T>> items;
  final Pagination pagination;
  final String? since;

  const SyncResult({
    required this.items,
    required this.pagination,
    this.since,
  });
}

enum RecordSyncMode { patch, update }

extension RecordSyncModeValue on RecordSyncMode {
  String get value => name;

  static RecordSyncMode parse(String? value,
      {RecordSyncMode fallback = RecordSyncMode.patch}) {
    if (value == null) return fallback;
    switch (value.toLowerCase().trim()) {
      case 'update':
        return RecordSyncMode.update;
      case 'patch':
        return RecordSyncMode.patch;
      default:
        return fallback;
    }
  }
}

@immutable
class CollectionSyncEntry {
  final String name;
  final CollectionSchemaDefinition? schema;
  final PrimaryKeyConfig? primaryKey;
  final List<Map<String, dynamic>> records;
  final RecordSyncMode? recordsMode;

  const CollectionSyncEntry({
    required this.name,
    this.schema,
    this.primaryKey,
    this.records = const [],
    this.recordsMode,
  });
}

enum CollectionSyncStatus { created, updated, unchanged, skipped, failed }

@immutable
class RecordSyncStats {
  final int created;
  final int updated;
  final int unchanged;
  final int skipped;
  final int failed;

  const RecordSyncStats({
    this.created = 0,
    this.updated = 0,
    this.unchanged = 0,
    this.skipped = 0,
    this.failed = 0,
  });

  int get total => created + updated + unchanged + skipped + failed;

  RecordSyncStats add({
    int created = 0,
    int updated = 0,
    int unchanged = 0,
    int skipped = 0,
    int failed = 0,
  }) {
    return RecordSyncStats(
      created: this.created + created,
      updated: this.updated + updated,
      unchanged: this.unchanged + unchanged,
      skipped: this.skipped + skipped,
      failed: this.failed + failed,
    );
  }
}

@immutable
class CollectionSyncReport {
  final String name;
  final CollectionSyncStatus status;
  final RecordSyncStats recordStats;
  final Object? error;

  const CollectionSyncReport({
    required this.name,
    required this.status,
    this.recordStats = const RecordSyncStats(),
    this.error,
  });
}

@immutable
class CollectionSyncResult {
  final List<CollectionSyncReport> reports;
  final int created;
  final int updated;
  final int unchanged;
  final int skipped;
  final int failed;
  final RecordSyncStats recordTotals;

  const CollectionSyncResult({
    required this.reports,
    this.created = 0,
    this.updated = 0,
    this.unchanged = 0,
    this.skipped = 0,
    this.failed = 0,
    this.recordTotals = const RecordSyncStats(),
  });

  bool get hasFailures => failed > 0 || recordTotals.failed > 0;
}

class CollectionSyncException implements Exception {
  final String message;
  final CollectionSyncResult result;

  CollectionSyncException(this.message, this.result);

  @override
  String toString() => 'CollectionSyncException($message)';
}

class RecordSyncException implements Exception {
  final String message;
  final RecordSyncStats stats;

  RecordSyncException(this.message, this.stats);

  @override
  String toString() => 'RecordSyncException($message)';
}

@immutable
class CollectionDetails {
  final String id;
  final String tenantId;
  final String name;
  final String? appId;
  final Map<String, dynamic>? schema;
  final String? primaryKeyField;
  final String? primaryKeyType;
  final bool? primaryKeyAuto;
  final String createdAt;
  final String updatedAt;
  final String? deletedAt;

  const CollectionDetails({
    required this.id,
    required this.tenantId,
    required this.name,
    this.appId,
    this.schema,
    this.primaryKeyField,
    this.primaryKeyType,
    this.primaryKeyAuto,
    required this.createdAt,
    required this.updatedAt,
    this.deletedAt,
  });

  factory CollectionDetails.fromJson(Map<String, dynamic> json) {
    dynamic schemaValue;
    final schemaRaw = json['schema_json'];
    if (schemaRaw is String && schemaRaw.isNotEmpty) {
      try {
        schemaValue = jsonDecode(schemaRaw);
      } catch (_) {
        schemaValue = {'_raw': schemaRaw};
      }
    } else if (json['schema'] is Map<String, dynamic>) {
      schemaValue = json['schema'] as Map<String, dynamic>;
    }
    return CollectionDetails(
      id: json['id'] as String,
      tenantId: json['tenant_id'] as String,
      name: json['name'] as String,
      appId: json['app_id'] as String?,
      schema: schemaValue is Map<String, dynamic>
          ? Map<String, dynamic>.from(schemaValue)
          : null,
      primaryKeyField: json['primary_key_field'] as String?,
      primaryKeyType: json['primary_key_type'] as String?,
      primaryKeyAuto: json['primary_key_auto'] as bool?,
      createdAt: json['created_at'] as String,
      updatedAt: json['updated_at'] as String,
      deletedAt: json['deleted_at'] as String?,
    );
  }
}

@immutable
class AuthProfile {
  final String? tenantId;
  final String? tenantName;
  final String? appId;
  final String? appName;
  final String? status;
  final String? keyPrefix;
  final DateTime? createdAt;
  final DateTime? lastUsed;

  const AuthProfile({
    this.tenantId,
    this.tenantName,
    this.appId,
    this.appName,
    this.status,
    this.keyPrefix,
    this.createdAt,
    this.lastUsed,
  });

  factory AuthProfile.fromJson(Map<String, dynamic> json) {
    return AuthProfile(
      tenantId: json['tenant_id'] as String?,
      tenantName: json['tenant_name'] as String?,
      appId: json['app_id'] as String?,
      appName: json['app_name'] as String?,
      status: json['status'] as String?,
      keyPrefix: json['key_prefix'] as String?,
      createdAt: _parseDate(json['created_at']),
      lastUsed: _parseDate(json['last_used']),
    );
  }

  static DateTime? _parseDate(dynamic value) {
    if (value == null) return null;
    if (value is DateTime) {
      return value.toUtc();
    }
    if (value is int) {
      return DateTime.fromMillisecondsSinceEpoch(value, isUtc: true);
    }
    if (value is String && value.isNotEmpty) {
      return DateTime.tryParse(value)?.toUtc();
    }
    return null;
  }

  JsonMap toJson() {
    return {
      if (tenantId != null) 'tenant_id': tenantId,
      if (tenantName != null) 'tenant_name': tenantName,
      if (appId != null) 'app_id': appId,
      if (appName != null) 'app_name': appName,
      if (status != null) 'status': status,
      if (keyPrefix != null) 'key_prefix': keyPrefix,
      if (createdAt != null) 'created_at': createdAt!.toUtc().toIso8601String(),
      if (lastUsed != null) 'last_used': lastUsed!.toUtc().toIso8601String(),
    };
  }
}

@immutable
class DocumentRecord<T extends Map<String, dynamic>> {
  final String id;
  final String tenantId;
  final String collectionId;
  final String key;
  final num? keyNumeric;
  final T data;
  final int version;
  final String createdAt;
  final String updatedAt;
  final String? deletedAt;

  const DocumentRecord({
    required this.id,
    required this.tenantId,
    required this.collectionId,
    required this.key,
    required this.data,
    required this.version,
    required this.createdAt,
    required this.updatedAt,
    this.keyNumeric,
    this.deletedAt,
  });
}

@immutable
class DocumentCount {
  final int count;
  final int? deletedCount;

  const DocumentCount({required this.count, this.deletedCount});

  factory DocumentCount.fromJson(Map<String, dynamic> json) {
    return DocumentCount(
      count: json['count'] as int,
      deletedCount: json['deleted_count'] as int?,
    );
  }
}

@immutable
class CollectionCount {
  final int count;
  final int? deletedCount;

  const CollectionCount({required this.count, this.deletedCount});
  factory CollectionCount.fromJson(Map<String, dynamic> json) {
    return CollectionCount(
      count: json['count'] as int,
      deletedCount: json['deleted_count'] as int?,
    );
  }
}

class TinyDBException implements Exception {
  final String message;
  final int status;
  final String? code;
  final dynamic details;
  final String? requestId;

  TinyDBException(this.message, this.status,
      {this.code, this.details, this.requestId});

  @override
  String toString() =>
      'TinyDBException(status: $status, code: $code, message: $message, requestId: $requestId)';
}

class TinyDBClient {
  final String _endpoint;
  final String _apiKey;
  final String? _appId;
  final http.Client _httpClient;
  final bool _ownsClient;
  final WebSocketConnector _webSocketConnector;

  TinyDBClient({
    required String endpoint,
    required String apiKey,
    String? appId,
    http.Client? httpClient,
    WebSocketConnector? webSocketConnector,
  })  : _endpoint = _normalizeEndpoint(endpoint),
        _apiKey = apiKey,
        _appId = appId,
        _httpClient = httpClient ?? http.Client(),
        _ownsClient = httpClient == null,
        _webSocketConnector = webSocketConnector ?? _defaultWebSocketConnector;

  static String _normalizeEndpoint(String endpoint) {
    final trimmed = endpoint.trim();
    if (trimmed.endsWith('/')) {
      return trimmed.substring(0, trimmed.length - 1);
    }
    return trimmed;
  }

  CollectionBuilder<T> collection<T extends Map<String, dynamic>>(
    String name,
  ) {
    if (name.trim().isEmpty) {
      throw ArgumentError('collection name is required');
    }
    return CollectionBuilder<T>(this, name.trim());
  }

  Future<List<CollectionDetails>> collections() async {
    final response = await _request<List<dynamic>>(
      method: 'GET',
      path: '/api/collections',
    );
    return response
        .cast<Map<String, dynamic>>()
        .map(CollectionDetails.fromJson)
        .toList(growable: false);
  }

  Future<CollectionCount> countCollections(
      {bool includeDeleted = false}) async {
    final response = await _request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/collections/count',
      query: {
        'include_deleted': includeDeleted.toString(),
      },
    );
    return CollectionCount.fromJson(response);
  }

  Future<CollectionDetails> describeCollection(String name) async {
    final all = await collections();
    final lowered = name.toLowerCase();
    final match =
        all.firstWhere((c) => c.name.toLowerCase() == lowered, orElse: () {
      throw TinyDBException(
        'Collection $name not found',
        404,
        code: 'collection_not_found',
      );
    });
    return match;
  }

  /// Fetches the authentication profile associated with the configured API key.
  Future<AuthProfile> me() async {
    final response = await _request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/me',
    );
    return AuthProfile.fromJson(response);
  }

  Future<CollectionDetails> ensureCollection({
    required String name,
    CollectionSchemaDefinition? schema,
    PrimaryKeyConfig? primaryKey,
    bool? sync,
  }) async {
    final body = <String, dynamic>{'name': name};
    final schemaJson = schema != null ? jsonEncode(schema.toJson()) : null;
    if (schemaJson != null) {
      body['schema'] = schemaJson;
    }
    if (_appId != null) {
      body['app_id'] = _appId;
    }
    if (primaryKey != null) {
      final pkJson = primaryKey.toJson();
      if (pkJson.isNotEmpty) {
        body['primary_key'] = pkJson;
      }
    }
    if (sync != null) {
      body['sync'] = sync;
    }

    try {
      final created = await _request<Map<String, dynamic>>(
        method: 'POST',
        path: '/api/collections',
        body: body,
      );
      return CollectionDetails.fromJson(created);
    } on TinyDBException catch (error) {
      if (error.status == 409) {
        if (schemaJson != null) {
          final updated = await _request<Map<String, dynamic>>(
            method: 'PUT',
            path: '/api/collections/${Uri.encodeComponent(name)}',
            body: {'schema': schemaJson},
          );
          return CollectionDetails.fromJson(updated);
        }
        return describeCollection(name);
      }
      rethrow;
    }
  }

  Future<CollectionSyncResult> syncCollections(
    List<CollectionSyncEntry> entries, {
    RecordSyncMode defaultRecordMode = RecordSyncMode.patch,
  }) async {
    if (entries.isEmpty) {
      throw ArgumentError('entries cannot be empty');
    }

    final reports = <CollectionSyncReport>[];
    var created = 0;
    var updated = 0;
    var unchanged = 0;
    var skipped = 0;
    var failed = 0;
    var recordTotals = const RecordSyncStats();
    const equality = DeepCollectionEquality();

    for (final entry in entries) {
      final trimmedName = entry.name.trim();
      if (trimmedName.isEmpty) {
        skipped++;
        reports.add(
          CollectionSyncReport(
            name: entry.name,
            status: CollectionSyncStatus.skipped,
            error: ArgumentError('collection name cannot be empty'),
          ),
        );
        continue;
      }

      var existed = true;
      CollectionDetails? before;
      try {
        before = await describeCollection(trimmedName);
      } on TinyDBException catch (error) {
        if (error.status == 404) {
          existed = false;
        } else {
          failed++;
          reports.add(
            CollectionSyncReport(
              name: trimmedName,
              status: CollectionSyncStatus.failed,
              error: error,
            ),
          );
          continue;
        }
      }

      CollectionBuilder<JsonMap> builder = collection<JsonMap>(trimmedName);
      if (entry.schema != null) {
        builder = builder.schema(entry.schema!);
      }
      if (entry.primaryKey != null) {
        builder = builder.primaryKey(entry.primaryKey!);
      }

      CollectionClient<JsonMap> collectionClient;
      try {
        collectionClient = await builder.sync();
      } on TinyDBException catch (error) {
        failed++;
        reports.add(
          CollectionSyncReport(
            name: trimmedName,
            status: CollectionSyncStatus.failed,
            error: error,
          ),
        );
        continue;
      }

      final schemaChanged =
          _schemaDefinitionChanged(entry.schema, before, equality);
      final pkChanged = _primaryKeyConfigChanged(entry.primaryKey, before);

      var status = CollectionSyncStatus.unchanged;
      if (!existed) {
        status = CollectionSyncStatus.created;
      } else if (schemaChanged || pkChanged) {
        status = CollectionSyncStatus.updated;
      }

      RecordSyncStats recordStats = const RecordSyncStats();
      Object? statusError;
      final records = entry.records;
      final mode = entry.recordsMode ?? defaultRecordMode;
      if (records.isNotEmpty) {
        try {
          recordStats = await _syncCollectionRecords(
            collectionClient,
            records,
            mode,
            equality,
          );
        } on RecordSyncException catch (error) {
          recordStats = error.stats;
          statusError = error;
          status = CollectionSyncStatus.failed;
        } on TinyDBException catch (error) {
          statusError = error;
          status = CollectionSyncStatus.failed;
        }
      }

      recordTotals = recordTotals.add(
        created: recordStats.created,
        updated: recordStats.updated,
        unchanged: recordStats.unchanged,
        skipped: recordStats.skipped,
        failed: recordStats.failed,
      );

      switch (status) {
        case CollectionSyncStatus.created:
          created++;
          break;
        case CollectionSyncStatus.updated:
          updated++;
          break;
        case CollectionSyncStatus.unchanged:
          unchanged++;
          break;
        case CollectionSyncStatus.skipped:
          skipped++;
          break;
        case CollectionSyncStatus.failed:
          failed++;
          break;
      }

      reports.add(
        CollectionSyncReport(
          name: trimmedName,
          status: status,
          recordStats: recordStats,
          error: statusError,
        ),
      );
    }

    final result = CollectionSyncResult(
      reports: reports,
      created: created,
      updated: updated,
      unchanged: unchanged,
      skipped: skipped,
      failed: failed,
      recordTotals: recordTotals,
    );

    if (result.hasFailures) {
      throw CollectionSyncException(
        'Failed to sync ${result.failed + result.recordTotals.failed} collection or record operation(s)',
        result,
      );
    }

    return result;
  }

  Future<void> close() async {
    if (_ownsClient) {
      _httpClient.close();
    }
  }

  Future<R> _request<R>({
    required String method,
    required String path,
    Map<String, String?>? query,
    Object? body,
    R Function(dynamic data)? transform,
    bool expectResponseBody = true,
  }) async {
    final uri = _buildUri(path, query);
    final request = http.Request(method, uri);
    request.headers['Accept'] = 'application/json';
    request.headers['X-API-Key'] = _apiKey;
    request.headers['user-agent'] =
        'tinydb-sdk-dart/v${VersionReader.getVersion() ?? "0.1.0"} (+https://github.com/cubetiqlabs/tinydb-sdk-dart)';
    if (_appId != null) {
      request.headers['X-App-ID'] = _appId!;
    }
    if (body != null) {
      if (body is String) {
        request.headers['Content-Type'] =
            request.headers['Content-Type'] ?? 'application/json';
        request.body = body;
      } else if (body is List<int>) {
        request.bodyBytes = body;
      } else {
        request.headers['Content-Type'] = 'application/json';
        request.body = jsonEncode(body);
      }
    }

    http.Response response;
    try {
      final streamed = await _httpClient.send(request);
      response = await http.Response.fromStream(streamed);
    } catch (error) {
      throw TinyDBException('Network error: $error', 0);
    }

    if (response.statusCode >= 400) {
      throw _parseError(response);
    }

    if (!expectResponseBody || response.body.isEmpty) {
      return (null as R);
    }

    dynamic decoded;
    try {
      decoded = jsonDecode(response.body);
    } catch (_) {
      decoded = response.body;
    }

    if (transform != null) {
      return transform(decoded);
    }

    return decoded as R;
  }

  Uri _buildUri(String path, Map<String, String?>? query) {
    final normalized = path.startsWith('/') ? path : '/$path';
    final uri = Uri.parse('$_endpoint$normalized');
    if (query == null || query.isEmpty) {
      return uri;
    }
    final filtered = <String, String>{};
    for (final entry in query.entries) {
      final value = entry.value;
      if (value != null) {
        filtered[entry.key] = value;
      }
    }
    return uri.replace(queryParameters: {...uri.queryParameters, ...filtered});
  }

  Uri _buildWebSocketUri(
    String path, {
    Map<String, String?>? query,
  }) {
    final base = Uri.parse(_endpoint);
    final scheme = base.scheme == 'https' ? 'wss' : 'ws';
    final normalized = path.startsWith('/') ? path : '/$path';
    final uri = Uri(
      scheme: scheme,
      userInfo: base.userInfo,
      host: base.host,
      port: base.hasPort ? base.port : null,
      path: normalized,
    );
    if (query == null || query.isEmpty) {
      return uri;
    }
    final filtered = <String, String>{};
    for (final entry in query.entries) {
      final value = entry.value;
      if (value != null && value.isNotEmpty) {
        filtered[entry.key] = value;
      }
    }
    return uri.replace(queryParameters: filtered.isEmpty ? null : filtered);
  }

  WebSocketChannel _connectWebSocket(
    String path, {
    Map<String, String?>? query,
    Iterable<String>? protocols,
    Duration? pingInterval,
  }) {
    final uri = _buildWebSocketUri(path, query: query);
    final headers = <String, dynamic>{
      'authorization': 'Bearer $_apiKey',
      'user-agent':
          'tinydb-sdk-dart/v${VersionReader.getVersion() ?? "0.1.0"} (+https://github.com/cubetiqlabs/tinydb-sdk-dart)',
    };
    if (_appId != null && _appId!.isNotEmpty) {
      headers['x-app-id'] = _appId;
    }
    return _webSocketConnector(
      uri,
      headers: headers,
      protocols: protocols,
      pingInterval: pingInterval,
    );
  }

  TinyDBException _parseError(http.Response response) {
    dynamic payload;
    if (response.body.isNotEmpty) {
      try {
        payload = jsonDecode(response.body);
      } catch (_) {
        payload = null;
      }
    }
    final message = payload is Map<String, dynamic>
        ? (payload['message'] ??
            payload['error_description'] ??
            payload['error'] ??
            response.reasonPhrase ??
            'Request failed')
        : (response.reasonPhrase ?? 'Request failed');
    final code = payload is Map<String, dynamic> ? payload['code'] : null;
    return TinyDBException(
      message.toString(),
      response.statusCode,
      code: code?.toString(),
      details: payload,
      requestId: response.headers['x-request-id'],
    );
  }
}

class CollectionBuilder<T extends Map<String, dynamic>>
    implements Future<CollectionClient<T>> {
  final TinyDBClient _client;
  final String _name;
  CollectionSchemaDefinition? _schema;
  PrimaryKeyConfig? _primaryKey;

  CollectionBuilder(this._client, this._name);

  CollectionBuilder<T> schema(CollectionSchemaDefinition definition) {
    _schema = definition;
    return this;
  }

  CollectionBuilder<T> primaryKey(PrimaryKeyConfig config) {
    _primaryKey = config;
    return this;
  }

  Future<CollectionClient<T>> sync() async {
    final meta = await _client.ensureCollection(
      name: _name,
      schema: _schema,
      primaryKey: _primaryKey,
      sync: true,
    );
    return CollectionClient<T>(_client, _name, meta);
  }

  Future<CollectionClient<T>> _resolve() async {
    if (_schema != null || _primaryKey != null) {
      return sync();
    }
    final meta = await _client.ensureCollection(name: _name);
    return CollectionClient<T>(_client, _name, meta);
  }

  @override
  Stream<CollectionClient<T>> asStream() => _resolve().asStream();

  @override
  Future<CollectionClient<T>> catchError(Function onError,
          {bool Function(Object error)? test}) =>
      _resolve().catchError(onError, test: test);

  @override
  Future<R> then<R>(FutureOr<R> Function(CollectionClient<T> value) onValue,
          {Function? onError}) =>
      _resolve().then(onValue, onError: onError);

  @override
  Future<CollectionClient<T>> timeout(Duration timeLimit,
          {FutureOr<CollectionClient<T>> Function()? onTimeout}) =>
      _resolve().timeout(timeLimit, onTimeout: onTimeout);

  @override
  Future<CollectionClient<T>> whenComplete(FutureOr<void> Function() action) =>
      _resolve().whenComplete(action);
}

class ListOptions {
  final int? limit; // -1 means no limit
  final int? offset;
  final bool includeDeleted;
  final List<String>? select;
  final Map<String, dynamic>? filters;

  const ListOptions({
    this.limit,
    this.offset,
    this.includeDeleted = false,
    this.select,
    this.filters,
  });

  Map<String, String> toQuery() {
    final map = <String, String>{};
    if (limit != null) map['limit'] = '$limit';
    if (offset != null) map['offset'] = '$offset';
    if (includeDeleted) map['include_deleted'] = 'true';
    if (select != null && select!.isNotEmpty) {
      map['select'] = select!.join(',');
    }
    if (filters != null) {
      filters!.forEach((key, value) {
        if (value != null) {
          map['f.$key'] = value.toString();
        }
      });
    }
    return map;
  }
}

class CollectionClient<T extends Map<String, dynamic>> {
  final TinyDBClient _client;
  final String name;
  CollectionDetails _metadata;

  CollectionClient(this._client, this.name, this._metadata);

  CollectionDetails get details => _metadata;

  CollectionBuilder<T> schema(CollectionSchemaDefinition definition) {
    final builder = CollectionBuilder<T>(_client, name).schema(definition);
    final existingType = _metadata.primaryKeyType != null
        ? _primaryKeyTypeFromString(_metadata.primaryKeyType!)
        : null;
    if (_metadata.primaryKeyField != null ||
        existingType != null ||
        _metadata.primaryKeyAuto != null) {
      builder.primaryKey(PrimaryKeyConfig(
        field: _metadata.primaryKeyField,
        type: existingType,
        auto: _metadata.primaryKeyAuto,
      ));
    }
    return builder;
  }

  CollectionBuilder<T> primaryKey(PrimaryKeyConfig config) =>
      CollectionBuilder<T>(_client, name).primaryKey(config);

  Future<CollectionDetails> refresh() async {
    _metadata = await _client.describeCollection(name);
    return _metadata;
  }

  Future<ListResult<T>> list(
      {ListOptions options = const ListOptions()}) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents',
      query: options.toQuery(),
    );
    final items = (response['items'] as List<dynamic>? ?? [])
        .map((item) => _parseDocument<T>(item as Map<String, dynamic>))
        .toList(growable: false);
    return ListResult<T>(
      items: items,
      pagination: Pagination.fromJson(response['pagination']),
    );
  }

  Future<DocumentRecord<T>> get(String id, {bool pk = false}) => pk
      ? getByPrimaryKey(id)
      : _fetchDocument(
          '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id)}',
        );

  Future<DocumentCount> count({bool includeDeleted = false}) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents/count',
      query: {
        'include_deleted': includeDeleted.toString(),
      },
    );
    return DocumentCount.fromJson(response);
  }

  Future<DocumentRecord<T>> getByPrimaryKey(String key) => _fetchDocument(
        '/api/collections/${Uri.encodeComponent(name)}/documents/primary/${Uri.encodeComponent(key)}',
      );

  Future<DocumentRecord<T>> create(Map<String, dynamic> doc) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'POST',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents',
      body: doc,
    );
    return _parseDocument<T>(response);
  }

  Future<List<DocumentRecord<T>>> createMany(List<Map<String, dynamic>> docs,
      {bool?
          sync} // whether to sync the records upon creation (if pk exists, they will be updated instead)
      ) async {
    if (docs.isEmpty) return const [];
    final response = await _client._request<Map<String, dynamic>>(
      method: 'POST',
      path:
          '/api/collections/${Uri.encodeComponent(name)}/documents/bulk?sync=${sync == true ? 'true' : 'false'}',
      body: docs,
    );
    return (response['items'] as List<dynamic>? ?? [])
        .map((item) => _parseDocument<T>(item as Map<String, dynamic>))
        .toList(growable: false);
  }

  Future<DocumentRecord<T>> update(
    String id,
    Map<String, dynamic> doc,
  ) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'PUT',
      path:
          '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id)}',
      body: doc,
    );
    return _parseDocument<T>(response);
  }

  Future<DocumentRecord<T>> updateByPrimaryKey(
    String key,
    Map<String, dynamic> doc,
  ) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'PUT',
      path:
          '/api/collections/${Uri.encodeComponent(name)}/documents/primary/${Uri.encodeComponent(key)}',
      body: doc,
    );
    return _parseDocument<T>(response);
  }

  Future<DocumentRecord<T>> patch(
    String id,
    Map<String, dynamic> doc,
  ) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'PATCH',
      path:
          '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id)}',
      body: doc,
    );
    return _parseDocument<T>(response);
  }

  Future<DocumentRecord<T>> patchByPrimaryKey(
    String key,
    Map<String, dynamic> doc,
  ) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'PATCH',
      path:
          '/api/collections/${Uri.encodeComponent(name)}/documents/primary/${Uri.encodeComponent(key)}',
      body: doc,
    );
    return _parseDocument<T>(response);
  }

  Future<void> delete(dynamic id) async {
    if (id is Iterable) {
      for (final docId in id) {
        await delete(docId);
      }
      return;
    }
    await _client._request<void>(
      method: 'DELETE',
      path:
          '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id.toString())}',
      expectResponseBody: false,
    );
  }

  Future<void> deleteByPrimaryKey(dynamic key, {bool purge = false}) async {
    if (key is Iterable) {
      for (final k in key) {
        await deleteByPrimaryKey(k);
      }
      return;
    }
    await _client._request<void>(
      method: 'DELETE',
      path:
          '/api/collections/${Uri.encodeComponent(name)}/documents/primary/${Uri.encodeComponent(key)}?purge=${purge ? 'true' : 'false'}',
      expectResponseBody: false,
    );
  }

  Future<void> purge(dynamic id) async {
    if (id is Iterable) {
      for (final docId in id) {
        await purge(docId);
      }
      return;
    }
    await _client._request<void>(
      method: 'DELETE',
      path:
          '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id.toString())}/purge',
      query: {'confirm': 'true'},
      expectResponseBody: false,
    );
  }

  Future<RecordSyncStats> syncDocuments(
    List<Map<String, dynamic>> records, {
    RecordSyncMode mode = RecordSyncMode.patch,
  }) async {
    const equality = DeepCollectionEquality();
    return _syncCollectionRecords<T>(this, records, mode, equality);
  }

  Future<QueryResult<T>> query(Map<String, dynamic> request) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'POST',
      path: '/api/collections/${Uri.encodeComponent(name)}/query',
      body: request,
    );
    final items = (response['items'] as List<dynamic>? ?? [])
        .map((item) => _parseDocument<T>(item as Map<String, dynamic>))
        .toList(growable: false);
    return QueryResult<T>(
      items: items,
      pagination: Pagination.fromJson(response['pagination']),
    );
  }

  Future<AggregatedQueryResult<T>> queryAll(
    Map<String, dynamic> request, {
    int? pageLimit,
    int? maxPages,
    int? maxItems,
    void Function(QueryResult<T> page)? onPage,
    void Function(QueryProgress progress)? onProgress,
    CancellationToken? cancellationToken,
  }) async {
    if (maxPages != null && maxPages <= 0) {
      return AggregatedQueryResult<T>(
        items: List<DocumentRecord<T>>.empty(growable: false),
        pageCount: 0,
        exhausted: false,
        lastPagination: const Pagination(),
      );
    }

    if (maxItems != null && maxItems <= 0) {
      return AggregatedQueryResult<T>(
        items: List<DocumentRecord<T>>.empty(growable: false),
        pageCount: 0,
        exhausted: false,
        lastPagination: const Pagination(),
      );
    }

    final baseRequest = Map<String, dynamic>.from(request);
    if (pageLimit != null) {
      baseRequest['limit'] = pageLimit;
    }

    String? cursor = baseRequest['cursor'] as String?;
    final collected = <DocumentRecord<T>>[];
    var pageCount = 0;
    var exhausted = true;
    var lastPagination = const Pagination();
    final token = cancellationToken ?? CancellationToken.none;

    while (true) {
      if (token.isCancelled) {
        exhausted = false;
        onProgress?.call(QueryProgress(
          pageCount: pageCount,
          itemCount: collected.length,
          lastPagination: lastPagination,
          done: true,
          maxPages: maxPages,
          maxItems: maxItems,
        ));
        break;
      }

      final pageRequest = Map<String, dynamic>.from(baseRequest);
      if (cursor != null && cursor.isNotEmpty) {
        pageRequest['cursor'] = cursor;
      } else {
        pageRequest.remove('cursor');
      }

      final page = await query(pageRequest);
      pageCount += 1;
      onPage?.call(page);
      lastPagination = page.pagination;

      if (page.items.isNotEmpty) {
        collected.addAll(page.items);
      }

      cursor = page.pagination.nextCursor;
      final hasCursor = cursor != null && cursor.isNotEmpty;

      if (maxItems != null && collected.length > maxItems) {
        collected.removeRange(maxItems, collected.length);
      }

      final reachedMaxItems = maxItems != null && collected.length >= maxItems;
      final reachedMaxPages = maxPages != null && pageCount >= maxPages;
      final cancelled = token.isCancelled;
      final done =
          cancelled || !hasCursor || reachedMaxPages || reachedMaxItems;

      onProgress?.call(QueryProgress(
        pageCount: pageCount,
        itemCount: collected.length,
        lastPagination: page.pagination,
        done: done,
        maxPages: maxPages,
        maxItems: maxItems,
      ));

      if (cancelled) {
        exhausted = false;
        break;
      }

      if (!hasCursor) {
        exhausted = true;
        break;
      }

      if (reachedMaxPages) {
        exhausted = false;
        break;
      }

      if (reachedMaxItems) {
        exhausted = false;
        break;
      }
    }

    return AggregatedQueryResult<T>(
      items: List<DocumentRecord<T>>.unmodifiable(collected),
      pageCount: pageCount,
      exhausted: exhausted,
      lastPagination: lastPagination,
    );
  }

  Stream<QueryResult<T>> queryPages(
    Map<String, dynamic> request, {
    int? pageLimit,
    int? maxPages,
    int? maxItems,
    void Function(QueryProgress progress)? onProgress,
    CancellationToken? cancellationToken,
  }) async* {
    if (maxPages != null && maxPages <= 0) {
      return;
    }
    if (maxItems != null && maxItems <= 0) {
      return;
    }

    final baseRequest = Map<String, dynamic>.from(request);
    if (pageLimit != null) {
      baseRequest['limit'] = pageLimit;
    }

    String? cursor = baseRequest['cursor'] as String?;
    var pageCount = 0;
    var emittedItems = 0;
    Pagination lastPagination = const Pagination();
    final token = cancellationToken ?? CancellationToken.none;

    while (true) {
      if (token.isCancelled) {
        onProgress?.call(QueryProgress(
          pageCount: pageCount,
          itemCount: emittedItems,
          lastPagination: lastPagination,
          done: true,
          maxPages: maxPages,
          maxItems: maxItems,
        ));
        break;
      }

      if (maxItems != null && emittedItems >= maxItems) {
        onProgress?.call(QueryProgress(
          pageCount: pageCount,
          itemCount: emittedItems,
          lastPagination: lastPagination,
          done: true,
          maxPages: maxPages,
          maxItems: maxItems,
        ));
        break;
      }

      final pageRequest = Map<String, dynamic>.from(baseRequest);
      if (cursor != null && cursor.isNotEmpty) {
        pageRequest['cursor'] = cursor;
      } else {
        pageRequest.remove('cursor');
      }

      final page = await query(pageRequest);
      pageCount += 1;
      lastPagination = page.pagination;

      final nextCursor = page.pagination.nextCursor;
      final hasCursor = nextCursor != null && nextCursor.isNotEmpty;

      var remainingItems =
          maxItems != null ? maxItems - emittedItems : page.items.length;
      if (remainingItems < 0) {
        remainingItems = 0;
      }
      final shouldTrim = maxItems != null && remainingItems < page.items.length;
      final itemsToEmit = shouldTrim
          ? page.items.take(remainingItems).toList(growable: false)
          : page.items;
      final emitPage = shouldTrim
          ? QueryResult<T>(items: itemsToEmit, pagination: page.pagination)
          : page;

      emittedItems += itemsToEmit.length;
      if (itemsToEmit.isNotEmpty || !shouldTrim) {
        yield emitPage;
      }

      final reachedMaxPages = maxPages != null && pageCount >= maxPages;
      final reachedMaxItems = maxItems != null && emittedItems >= maxItems;
      final done =
          token.isCancelled || !hasCursor || reachedMaxPages || reachedMaxItems;

      onProgress?.call(QueryProgress(
        pageCount: pageCount,
        itemCount: emittedItems,
        lastPagination: page.pagination,
        done: done,
        maxPages: maxPages,
        maxItems: maxItems,
      ));

      if (done) {
        break;
      }

      cursor = nextCursor;
    }
  }

  Stream<DocumentRecord<T>> queryStream(
    Map<String, dynamic> request, {
    int? pageLimit,
    int? maxPages,
    int? maxItems,
    void Function(QueryProgress progress)? onProgress,
    CancellationToken? cancellationToken,
  }) async* {
    final token = cancellationToken ?? CancellationToken.none;
    await for (final page in queryPages(
      request,
      pageLimit: pageLimit,
      maxPages: maxPages,
      maxItems: maxItems,
      cancellationToken: token,
      onProgress: onProgress,
    )) {
      for (final doc in page.items) {
        if (token.isCancelled) {
          return;
        }
        yield doc;
      }
      if (token.isCancelled) {
        return;
      }
    }
  }

  Stream<CollectionWatchEvent<T>> watch({
    bool includeInitial = true,
    Map<String, dynamic>? initialQuery,
    int? initialPageLimit,
    int? initialMaxPages,
    int? initialMaxItems,
    void Function(QueryProgress progress)? onProgress,
    CancellationToken? cancellationToken,
    Duration? pingInterval,
  }) {
    final token = cancellationToken ?? CancellationToken.none;
    final controller = StreamController<CollectionWatchEvent<T>>();
    WebSocketChannel? channel;
    StreamSubscription? subscription;
    var closing = false;
    var pageCount = 0;
    var totalItems = 0;
    var lastPagination = const Pagination();

    Future<void> closeController({bool notifyProgress = true}) async {
      if (closing) {
        return;
      }
      closing = true;
      try {
        await subscription?.cancel();
      } catch (_) {}
      try {
        await channel?.sink.close();
      } catch (_) {}
      if (notifyProgress) {
        onProgress?.call(QueryProgress(
          pageCount: pageCount,
          itemCount: totalItems,
          lastPagination: lastPagination,
          done: true,
        ));
      }
      if (!controller.isClosed) {
        await controller.close();
      }
    }

    token.whenCancelled.then((_) => closeController());

    Future<void> pumpInitial() async {
      if (!includeInitial) {
        return;
      }

      final request = Map<String, dynamic>.from(initialQuery ?? const {});
      try {
        await for (final record in queryStream(
          request,
          pageLimit: initialPageLimit,
          maxPages: initialMaxPages,
          maxItems: initialMaxItems,
          cancellationToken: token,
          onProgress: (progress) {
            pageCount = progress.pageCount;
            totalItems = progress.itemCount;
            lastPagination = progress.lastPagination;
            onProgress?.call(progress);
          },
        )) {
          if (token.isCancelled || controller.isClosed) {
            break;
          }
          controller.add(CollectionWatchEvent<T>.initial(record));
        }
      } catch (error, stackTrace) {
        if (!controller.isClosed) {
          controller.addError(error, stackTrace);
        }
        rethrow;
      }
    }

    void handleRealtimeMessage(dynamic message) {
      if (controller.isClosed) {
        return;
      }
      final payload = _decodeRealtimeMessage(message);
      if (payload == null) {
        return;
      }

      if (payload['ok'] == true) {
        controller.add(CollectionWatchEvent<T>.ack(payload));
        onProgress?.call(QueryProgress(
          pageCount: pageCount,
          itemCount: totalItems,
          lastPagination: lastPagination,
          done: token.isCancelled,
        ));
        return;
      }

      final kind = _mapRealtimeKind(payload['type']?.toString());
      if (kind == CollectionWatchEventKind.ack) {
        controller.add(CollectionWatchEvent<T>.ack(payload));
        onProgress?.call(QueryProgress(
          pageCount: pageCount,
          itemCount: totalItems,
          lastPagination: lastPagination,
          done: token.isCancelled,
        ));
        return;
      }
      if (kind == CollectionWatchEventKind.error) {
        controller.add(CollectionWatchEvent<T>.change(
          kind: CollectionWatchEventKind.error,
          raw: payload,
        ));
        return;
      }

      if (kind == CollectionWatchEventKind.keepalive) {
        controller.add(CollectionWatchEvent<T>.keepalive(payload));
        return;
      }

      DocumentRecord<T>? document;
      if (payload['document'] is Map<String, dynamic>) {
        document =
            _parseDocument<T>(payload['document'] as Map<String, dynamic>);
      }

      final T? data = _castRealtimeData(payload['data']);
      final id = payload['id']?.toString();
      DateTime? timestamp;
      final ts = payload['ts'];
      if (ts is String) {
        timestamp = DateTime.tryParse(ts);
      } else if (ts is int) {
        timestamp = DateTime.fromMillisecondsSinceEpoch(ts, isUtc: true);
      }

      controller.add(CollectionWatchEvent<T>.change(
        kind: kind,
        documentId: document?.id ?? id,
        data: document?.data ?? data,
        document: document,
        timestamp: timestamp,
        raw: payload,
      ));

      if (kind != CollectionWatchEventKind.keepalive &&
          kind != CollectionWatchEventKind.ack) {
        totalItems += 1;
      }

      onProgress?.call(QueryProgress(
        pageCount: pageCount,
        itemCount: totalItems,
        lastPagination: lastPagination,
        done: token.isCancelled,
      ));
    }

    controller.onListen = () {
      Future<void>(() async {
        try {
          await pumpInitial();

          if (token.isCancelled) {
            await closeController();
            return;
          }

          channel = _client._connectWebSocket(
            '/subscribe/${Uri.encodeComponent(name)}',
            pingInterval: pingInterval,
          );

          subscription = channel!.stream.listen(
            handleRealtimeMessage,
            onError: (error, stackTrace) async {
              if (!controller.isClosed) {
                controller.addError(error, stackTrace);
              }
              await closeController();
            },
            onDone: () async {
              await closeController();
            },
            cancelOnError: false,
          );
        } catch (error, stackTrace) {
          if (!controller.isClosed) {
            controller.addError(error, stackTrace);
          }
          await closeController();
        }
      });
    };

    controller.onCancel = () => closeController();

    return controller.stream;
  }

  static Map<String, dynamic>? _decodeRealtimeMessage(dynamic message) {
    try {
      if (message is String) {
        final decoded = jsonDecode(message);
        if (decoded is Map<String, dynamic>) {
          return Map<String, dynamic>.from(decoded);
        }
      } else if (message is List<int>) {
        final decoded = jsonDecode(utf8.decode(message));
        if (decoded is Map<String, dynamic>) {
          return Map<String, dynamic>.from(decoded);
        }
      } else if (message is Map<String, dynamic>) {
        return Map<String, dynamic>.from(message);
      }
    } catch (_) {
      return null;
    }
    return null;
  }

  static CollectionWatchEventKind _mapRealtimeKind(String? type) {
    switch (type?.toLowerCase()) {
      case 'create':
        return CollectionWatchEventKind.create;
      case 'update':
        return CollectionWatchEventKind.update;
      case 'delete':
        return CollectionWatchEventKind.delete;
      case 'keepalive':
      case 'ping':
        return CollectionWatchEventKind.keepalive;
      case 'ack':
        return CollectionWatchEventKind.ack;
      default:
        return CollectionWatchEventKind.error;
    }
  }

  T? _castRealtimeData(dynamic value) {
    if (value == null) {
      return null;
    }
    if (value is T) {
      return value;
    }
    if (value is Map<String, dynamic>) {
      return Map<String, dynamic>.from(value) as T;
    }
    if (value is String && value.isNotEmpty) {
      try {
        final decoded = jsonDecode(value);
        if (decoded is Map<String, dynamic>) {
          return Map<String, dynamic>.from(decoded) as T;
        }
      } catch (_) {
        return null;
      }
    }
    return null;
  }

  Future<SyncResult<T>> sync([SyncParams params = const SyncParams()]) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/collections/${Uri.encodeComponent(name)}/sync',
      query: params.toQuery(),
    );
    final items = (response['items'] as List<dynamic>? ?? [])
        .map((item) => item as Map<String, dynamic>)
        .map((item) => SyncChange<T>(
              changeType: item['change_type'] as String,
              document:
                  _parseDocument<T>(item['document'] as Map<String, dynamic>),
            ))
        .toList(growable: false);
    return SyncResult<T>(
      items: items,
      pagination: Pagination.fromJson(response['pagination']),
      since: response['since'] as String?,
    );
  }

  Future<dynamic> schemaJson() async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/collections/${Uri.encodeComponent(name)}/schema',
    );
    return response['schema'];
  }

  Future<DocumentRecord<T>> _fetchDocument(String path) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: path,
    );
    return _parseDocument<T>(response);
  }
}

PrimaryKeyType? _primaryKeyTypeFromString(String value) {
  for (final type in PrimaryKeyType.values) {
    if (type.value == value) {
      return type;
    }
  }
  return null;
}

DocumentRecord<T> _parseDocument<T extends Map<String, dynamic>>(
  Map<String, dynamic> payload,
) {
  Map<String, dynamic> data = {};
  final rawData = payload['data'];
  if (rawData is String && rawData.isNotEmpty) {
    try {
      final decoded = jsonDecode(rawData);
      if (decoded is Map<String, dynamic>) {
        data = Map<String, dynamic>.from(decoded);
      }
    } catch (_) {
      data = {'_raw': rawData};
    }
  } else if (rawData is Map<String, dynamic>) {
    data = Map<String, dynamic>.from(rawData);
  }

  final docId = (data['_doc_id'] ?? payload['id'])?.toString();
  if (docId != null) {
    data['_doc_id'] = docId;
  }

  final rawVersion = payload['version'];
  var version = 1;
  if (rawVersion is num) {
    version = rawVersion.round();
  } else if (rawVersion is String) {
    final parsed = int.tryParse(rawVersion);
    if (parsed != null) {
      version = parsed;
    }
  }

  return DocumentRecord<T>(
    id: payload['id'] as String,
    tenantId: payload['tenant_id'] as String,
    collectionId: payload['collection_id'] as String,
    key: payload['key'] as String,
    keyNumeric:
        payload['key_numeric'] is num ? payload['key_numeric'] as num : null,
    data: Map<String, dynamic>.from(data) as T,
    version: version,
    createdAt: payload['created_at'] as String,
    updatedAt: payload['updated_at'] as String,
    deletedAt: payload['deleted_at'] as String?,
  );
}

bool _schemaDefinitionChanged(
  CollectionSchemaDefinition? desired,
  CollectionDetails? existing,
  DeepCollectionEquality equality,
) {
  if (desired == null) {
    return false;
  }
  final desiredMap = desired.toJson();
  final existingMap = existing?.schema;
  if (existingMap == null) {
    return desiredMap.isNotEmpty;
  }
  return !equality.equals(existingMap, desiredMap);
}

bool _primaryKeyConfigChanged(
    PrimaryKeyConfig? desired, CollectionDetails? existing) {
  if (desired == null || existing == null) {
    return false;
  }
  final desiredField = desired.field?.trim();
  final desiredType = desired.type?.value.trimmedLower();
  final existingField = existing.primaryKeyField?.trim();
  final existingType = existing.primaryKeyType?.trim();

  if (desiredField != null && desiredField.isNotEmpty) {
    if ((existingField ?? '').trim().toLowerCase() !=
        desiredField.toLowerCase()) {
      return true;
    }
  }

  if (desiredType != null && desiredType.isNotEmpty) {
    if ((existingType ?? '').trim().toLowerCase() != desiredType) {
      return true;
    }
  }

  if (desired.auto != null && desired.auto != existing.primaryKeyAuto) {
    return true;
  }

  return false;
}

extension _TinyDBStringExtensions on String {
  String trimmedLower() => trim().toLowerCase();
}

Future<RecordSyncStats> _syncCollectionRecords<T extends Map<String, dynamic>>(
  CollectionClient<T> collection,
  List<Map<String, dynamic>> rawRecords,
  RecordSyncMode mode,
  DeepCollectionEquality equality,
) async {
  var stats = const RecordSyncStats();
  if (rawRecords.isEmpty) {
    return stats;
  }

  final keepPrimary = mode == RecordSyncMode.update;
  final pkField = _resolvePrimaryKeyField(collection.details);

  for (var index = 0; index < rawRecords.length; index++) {
    final record = Map<String, dynamic>.from(rawRecords[index]);
    final key = _extractDocumentKey(record, pkField);
    if (key == null || key.isEmpty) {
      stats = stats.add(skipped: 1);
      continue;
    }

    DocumentRecord<T>? existing;
    try {
      existing = await collection.getByPrimaryKey(key);
    } on TinyDBException catch (error) {
      if (error.status != 404) {
        stats = stats.add(failed: 1);
        continue;
      }
    }

    if (existing == null) {
      final payload = _prepareRecordCreatePayload(record);
      if (payload.isEmpty) {
        stats = stats.add(skipped: 1);
        continue;
      }
      try {
        await collection.create(payload);
        stats = stats.add(created: 1);
      } on TinyDBException {
        stats = stats.add(failed: 1);
      }
      continue;
    }

    final payload = _prepareRecordSyncPayload(record, pkField, keepPrimary);
    if (payload.isEmpty) {
      stats = stats.add(skipped: 1);
      continue;
    }

    final existingData = Map<String, dynamic>.from(existing.data);
    if (_shouldSkipRecord(
        existingData, payload, pkField, keepPrimary, equality, mode)) {
      stats = stats.add(unchanged: 1);
      continue;
    }

    try {
      if (mode == RecordSyncMode.update) {
        await collection.update(existing.id, payload);
      } else {
        await collection.patch(existing.id, payload);
      }
      stats = stats.add(updated: 1);
    } on TinyDBException {
      stats = stats.add(failed: 1);
    }
  }

  if (stats.failed > 0) {
    throw RecordSyncException(
      'Failed to sync ${stats.failed} record(s) for collection ${collection.name}',
      stats,
    );
  }

  return stats;
}

String _resolvePrimaryKeyField(CollectionDetails details) {
  final field = details.primaryKeyField?.trim();
  if (field == null || field.isEmpty) {
    return 'id';
  }
  return field;
}

String? _extractDocumentKey(Map<String, dynamic> record, String pkField) {
  final value = record[pkField];
  if (value == null) {
    return null;
  }
  final key = value.toString().trim();
  return key.isEmpty ? null : key;
}

Map<String, dynamic> _prepareRecordCreatePayload(Map<String, dynamic> record) {
  final payload = <String, dynamic>{};
  record.forEach((key, value) {
    if (key == '_doc_id') {
      return;
    }
    payload[key] = value;
  });
  return payload;
}

Map<String, dynamic> _prepareRecordSyncPayload(
  Map<String, dynamic> record,
  String pkField,
  bool keepPrimary,
) {
  final payload = <String, dynamic>{};
  record.forEach((key, value) {
    if (key == '_doc_id') {
      return;
    }
    if (!keepPrimary && key == pkField) {
      return;
    }
    payload[key] = value;
  });
  return payload;
}

Map<String, dynamic> _sanitizeExistingRecord(
  Map<String, dynamic> existing,
  String pkField,
  bool keepPrimary,
) {
  final sanitized = <String, dynamic>{};
  existing.forEach((key, value) {
    if (key == '_doc_id') {
      return;
    }
    if (!keepPrimary && key == pkField) {
      return;
    }
    sanitized[key] = value;
  });
  return sanitized;
}

bool _shouldSkipRecord(
  Map<String, dynamic> existing,
  Map<String, dynamic> payload,
  String pkField,
  bool keepPrimary,
  DeepCollectionEquality equality,
  RecordSyncMode mode,
) {
  final sanitizedExisting =
      _sanitizeExistingRecord(existing, pkField, keepPrimary);
  if (mode == RecordSyncMode.patch) {
    for (final entry in payload.entries) {
      if (!equality.equals(sanitizedExisting[entry.key], entry.value)) {
        return false;
      }
    }
    return true;
  }
  return equality.equals(sanitizedExisting, payload);
}
