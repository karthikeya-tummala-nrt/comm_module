import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:comm_module/transport/transport.dart';

class UdpTransport extends Transport {
  final InternetAddress address;
  final int port;

  final InternetAddress? remoteAddress;
  final int? remotePort;

  RawDatagramSocket? _udpSocket;
  final StreamController<Uint8List> _dataStreamController =
      StreamController<Uint8List>.broadcast();

  UdpTransport({
    required this.address,
    required this.port,
    this.remoteAddress,
    this.remotePort,
  });

  @override
  Stream<Uint8List> get onData => _dataStreamController.stream;

  @override
  bool get isConnected => _udpSocket != null;

  @override
  Future<void> connect() async {
    if (_udpSocket != null) {
      throw StateError("Transport already connected");
    }

    try {
      _udpSocket = await RawDatagramSocket.bind(address, port);
    } catch (e) {
      throw StateError('Failed to bind UDP Socket: $e');
    }

    _udpSocket!.listen(
      (event) {
        if (event == RawSocketEvent.read) {
          Datagram? datagram;
          while (true) {
            datagram = _udpSocket!.receive();
            if (datagram == null) break;

            if (!_dataStreamController.isClosed) {
              _dataStreamController.add(datagram.data);
            }
          }
        }
      },
      onDone: () {
        _udpSocket = null;
        if (!_dataStreamController.isClosed) _dataStreamController.close();
      },
      onError: _dataStreamController.addError,
    );
  }

  @override
  Future<void> send(Uint8List data) async {
    if (_udpSocket == null) {
      throw StateError(
        "Transport not connected. Call connect() before send().",
      );
    }

    if (remoteAddress == null || remotePort == null) {
      throw StateError(
        "Cannot send: remoteAddress and remotePort must be provided in UdpTransport constructor.",
      );
    }

    _udpSocket!.send(data, remoteAddress!, remotePort!);
  }

  @override
  Future<void> close() async {
    if (_udpSocket == null) return;

    final socket = _udpSocket;
    _udpSocket = null;
    socket?.close();
  }

  @override
  Future<void> dispose() async {
    await close();
    await _dataStreamController.close();
  }

}
