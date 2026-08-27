package spectrum

import (
	"github.com/brentp/intintmap"
	spectrumpacket "github.com/cooldogedev/spectrum/server/packet"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

var internalPackets = []uint32{
	spectrumpacket.IDConnectionResponse,
	spectrumpacket.IDFlush,
	spectrumpacket.IDLatency,
	spectrumpacket.IDTransfer,
	spectrumpacket.IDUpdateCache,
	spectrumpacket.IDBackendReady,

	packet.IDAddActor,
	packet.IDAddItemActor,
	packet.IDAddPainting,
	packet.IDAddPlayer,

	packet.IDBossEvent,

	packet.IDChunkRadiusUpdated,

	packet.IDItemRegistry,

	packet.IDMobEffect,

	packet.IDPlayerList,
	packet.IDPlayStatus,

	packet.IDRemoveActor,
	packet.IDRemoveObjective,

	packet.IDSetDisplayObjective,
	packet.IDStartGame,
	packet.IDDisconnect,
}

var packetMap *intintmap.Map

func RegisterPacketDecode(id uint32, value bool) {
	if value {
		packetMap.Put(int64(id), 1)
	} else {
		packetMap.Del(int64(id))
	}
}

func shouldDecodePacket(packet uint32) bool {
	_, ok := packetMap.Get(int64(packet))
	return ok
}

// shouldDecodePacketForProtocol keeps native sessions on Spectrum's raw fast
// path, but forces every historical session through the proxy's conversion
// boundary. LevelChunk and other packets may contain already mapped payloads,
// while their enclosing wire layouts still belong to the target protocol.
func shouldDecodePacketForProtocol(packetID uint32, proto minecraft.Protocol) bool {
	return proto.ID() != minecraft.DefaultProtocol.ID() || shouldDecodePacket(packetID)
}

func init() {
	packetMap = intintmap.New(len(internalPackets), 0.999)
	for _, id := range internalPackets {
		packetMap.Put(int64(id), 1)
	}
}
