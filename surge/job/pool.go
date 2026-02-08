package job

import "sync"

var jobEnvelopePool = sync.Pool{
	New: func() any {
		return &JobEnvelope{}
	},
}

func GetJobEnvelope() *JobEnvelope {
	return jobEnvelopePool.Get().(*JobEnvelope)
}

func PutJobEnvelope(j *JobEnvelope) {
	j.Reset()
	jobEnvelopePool.Put(j)
}
