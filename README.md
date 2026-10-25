# rapide
a (WIP) toy reliable UDP network protocol written in Go

## Features
- Basic multi-sending + independent sequencing (reliable & unreliable)
- Full connection lifecycle:
    - `rapide.Listen()`
    - `rapide.Dial()`
    - `Listener.Accept()`
    - `Listener.Addr()`
    - `Listener.Close()`
    - `Connection.Read()`
    - `Connection.Send()`
    - `Connection.LocalAddr()`
    - `Connection.RemoteAddr()`
    - `Connection.Close()`
- TCP's retransmission timer ([RFC 6298](https://www.rfc-editor.org/info/rfc6298/))