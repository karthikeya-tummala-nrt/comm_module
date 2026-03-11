import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:data_bridge/transport/transport.dart';

class TcpTransport extends Transport {
  final String host;
  final int port;
  Socket? _tcpSocket;
  final StreamController<Uint8List> _dataStreamController =
      StreamController<Uint8List>.broadcast();

  TcpTransport({required this.host, required this.port});

  @override
  Stream<Uint8List> get onData => _dataStreamController.stream;

  @override
  bool get isConnected => _tcpSocket != null;

  @override
  Future<void> connect() async {
    if (_tcpSocket != null) {
      throw StateError("Transport is already connected. Call close() first.");
    }

    try {
      _tcpSocket = await Socket.connect(host, port);
      _tcpSocket!.listen(
        (data) {
          _dataStreamController.add(data);
        },
        onError: _dataStreamController.addError,
        onDone: close,
      );
    } catch (e) {
      _tcpSocket = null;
      rethrow;
    }
  }

  @override
  Future<void> send(Uint8List data) async {
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
    if (_tcpSocket == null) return;

    final socket = _tcpSocket;
    _tcpSocket = null;
    socket?.destroy();
  }

  @override
  Future<void> dispose() async {
    await close();
    await _dataStreamController.close();
  }
}
