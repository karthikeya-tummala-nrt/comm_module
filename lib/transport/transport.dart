import 'dart:typed_data';

abstract class Transport {
  bool get isConnected;

  Stream<Uint8List> get onData;
  Future<void> connect();
  Future<void> send(Uint8List data);
  Future<void> close();
  Future<void> dispose();
}
