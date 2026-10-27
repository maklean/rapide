# rapide
a (WIP) toy reliable UDP network protocol written in Go

## Features
- [Multi-sending + independent sequencing](https://io7m.com/documents/udp-reliable/#ack-multi-sending-independent-sequence:~:text=independent%20sequencing.-,1.4.4%20ACK%20(Multi%20sending%2C%20independent%20sequence),-By%20once%20again) (reliable & unreliable)
    - Sequencing uses Serial Number Arithmetic ([RFC 1982](https://www.rfc-editor.org/info/rfc1982/))
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
- Flow Control ([explained here](https://github.com/maklean/rapide/pull/3#issue-6051682521))