# SpectrumDF
SpectrumDF is an implementation of the [Spectrum](https://github.com/cooldogedev/Spectrum) proxy for Dragonfly servers.

## Examples
You can find examples of how to use SpectrumDF in the [example](example) directory.

## Historical client protocols

Spectrum may keep its backend wire native while accepting historical clients.
Pass the same registry-aware protocol set used by the public Spectrum listener
to `NewListenerWithProtocols`. SpectrumDF exposes the selected protocol through
`Proto()` so Dragonfly can encode target block palettes before chunk cache
hashing. Native sessions retain raw packet forwarding; historical sessions are
decoded by Spectrum for bidirectional protocol conversion.
