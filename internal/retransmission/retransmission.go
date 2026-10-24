package retransmission

import (
	"math"

	"github.com/maklean/rapide/internal/config"
)

const alpha float64 = 1.0 / 8.0
const beta float64 = 1.0 / 4.0
const k float64 = 4

// RetransmissionTimer based on RFC 6298
type RetransmissionTimer struct {
	// current retransmission timeout (in milliseconds)
	RTO float64

	// smoothed round-trip time (in milliseconds)
	SRTT float64

	// round-trip time variation (in milliseconds)
	RTTVAR float64

	// at initial measurement?
	init bool
}

// RetransmissionTimerInit initalizes a new RetransmissionTimer.
func RetransmissionTimerInit() (rt RetransmissionTimer) {
	rt.RTO = 1000 // 1 second initial RTO
	rt.init = true
	return
}

// DoMeasurement computes the current RTT measurement (in milliseconds).
func (rt *RetransmissionTimer) DoMeasurement(rtt float64) {
	if rt.init {
		rt.SRTT = rtt
		rt.RTTVAR = rtt / 2.0

		rt.init = false
	} else {
		rt.RTTVAR = (1-beta)*rt.RTTVAR + beta*math.Abs(rt.SRTT-rtt)
		rt.SRTT = (1-alpha)*rt.SRTT + alpha*rtt
	}

	rt.RTO = max(1, rt.SRTT+max(float64(config.ConnectionTick), k*rt.RTTVAR))
}
