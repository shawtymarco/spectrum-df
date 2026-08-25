<p align="center">
  <img src="assets/go-gopher.png" alt="Go gopher" width="180">
</p>

<h1 align="center">SpectrumDF</h1>

<p align="center">The Dragonfly backend listener and fast-transfer bridge for Spectrum.</p>

## ✅ Supported Versions

SpectrumDF keeps its backend packet wire native while exposing the public
client protocol to Dragonfly for target-aware chunk palette encoding.

| Protocol ID | Minecraft version | Backend wire | BRBW automated E2E |
|------------:|-------------------|:------------:|:------------------:|
| 2169 | 1.26.45 | Native | ✅ |
| 2168 | 1.26.40-1.26.44 | Native + adapter context | ✅ |
| 1001 | 1.26.30-1.26.34, 1.26.36 | Native + adapter context | ✅ |
| 975 | 1.26.20, 1.26.21, 1.26.23 | Native + adapter context | ✅ |
| 844 | 1.21.110-1.21.114 | Native + adapter context | ✅ |
| 827 | 1.21.100-1.21.102 | Native + adapter context | ✅ |
| 486 | 1.18.10-1.18.12 | Native + adapter context | ✅ |

> [!NOTE]
> Spectrum performs wire conversion. SpectrumDF supplies the selected adapter
> to Dragonfly only where the live server state must be encoded per client.

## 🚀 Usage

Build the same registry-aware protocol set as the public Spectrum listener and
pass it to the backend listener:

```go
conf.AcceptedProtocolsProvider = func(blocks world.BlockRegistry) ([]minecraft.Protocol, error) {
	return multiversion.ProtocolsWithRegistries(blocks, server.VanillaItemEntries())
}

var listener *spectrum.Listener
conf.Listeners = []func(server.Config) (server.Listener, error){
	func(conf server.Config) (server.Listener, error) {
		var err error
		listener, err = spectrum.NewListenerWithProtocols(":19142", nil, conf.AcceptedProtocols)
		return listener, err
	},
}
```

Request a seamless backend switch without reconnecting the public Bedrock
client:

```go
err := listener.Transfer(playerUUID, "bedwars:19143")
```

`Transfer` keeps the client's RakNet, authentication, and resource-pack session
open while Spectrum replaces only the internal backend stream. `Flush` markers
and historical packet decoding are preserved across the bridge.

## 🔗 Dependencies

- [`shawtymarco/spectrum`](https://github.com/shawtymarco/spectrum) owns the
  public session, backend switching, packet conversion, and state reset.
- [`shawtymarco/dragonfly`](https://github.com/shawtymarco/dragonfly) consumes
  the optional protocol block-mapper capability before chunk cache hashing.
- [`shawtymarco/go-multiversion`](https://github.com/shawtymarco/go-multiversion)
  supplies registry-aware historical protocol implementations.
- [`shawtymarco/gophertunnel`](https://github.com/shawtymarco/gophertunnel)
  supplies the current native packet model.

## 🙏 Credits

- [cooldogedev/SpectrumDF](https://github.com/cooldogedev/spectrum-df)
- [cooldogedev/Spectrum](https://github.com/cooldogedev/spectrum)
- [df-mc/dragonfly](https://github.com/df-mc/dragonfly)
- [Sandertv/gophertunnel](https://github.com/Sandertv/gophertunnel)
- [Go gopher](https://go.dev/blog/gopher) by Renee French, used under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
