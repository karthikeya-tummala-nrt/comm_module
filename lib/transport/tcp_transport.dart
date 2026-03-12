import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:comm_module/transport/transport.dart';

class TcpTransport extends Transport {
  final String host;
  final int port;
  Socket? _tcpSocket;
  bool _disposed = false;
  StreamSubscription<Uint8List>? _tcpStreamSubscription;
  final StreamController<Uint8List> _dataStreamController =
      StreamController<Uint8List>.broadcast();

  TcpTransport({required this.host, required this.port});

  @override
  Stream<Uint8List> get onData => _dataStreamController.stream;

  @override
  bool get isConnected => _tcpSocket != null;

  @override
  Future<void> connect() async {
    if (_disposed) throw StateError('Transport already disposed. Create a new object.');

    if (_tcpSocket != null) {
      throw StateError("Transport is already connected. Call close() first.");
    }

    try {
      _tcpSocket = await Socket.connect(host, port);
      _tcpStreamSubscription = _tcpSocket!.listen(
        (data) {
          _dataStreamController.add(data);
        },
        onDone: () => close().catchError((e, s) {
          print('Error during close() $e\n$s');
        }),
        onError: _dataStreamController.addError,
      );
    } catch (e) {
      await close();
      rethrow;
    }
  }

  @override
  Future<void> send(Uint8List data) async {
    if (_disposed) throw StateError('Transport already disposed. Create a new object.');

    final socket = _tcpSocket;

    if (socket == null) {
      throw StateError(
        "Transport not connected. Call connect() before send().",
      );
    }

    try {
      socket.add(data);
      await socket.flush();
    } on SocketException catch (e) {
      throw StateError(
        "Failed to send data: socket closed or error occurred: $e",
      );
    }
  }

  @override
  Future<void> close() async {
    final socket = _tcpSocket;
    _tcpSocket = null;
    await _tcpStreamSubscription?.cancel();
    _tcpStreamSubscription = null;
    await socket?.close();
    socket?.destroy();
  }

  @override
  Future<void> dispose() async {
    _disposed = true;
    await close();
    await _dataStreamController.close();
  }
}
