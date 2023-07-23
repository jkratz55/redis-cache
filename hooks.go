package cache

// CompressionHook is a function type that is invoked prior to compressing or
// decompressing data.
type CompressionHook func(data []byte) ([]byte, error)

// Hook is an interface type defining the operations that can be intercepted
// and potentially allow for their behavior to be modified.
//
// The primary intention of Hook is to allow for observability: instrumentation,
// logging, tracing, etc.
//
// It is important implementations of Hook call next or the execution pipeline
// will terminate.
type Hook interface {
	MarshalHook(next Marshaller) Marshaller
	UnmarshallHook(next Unmarshaller) Unmarshaller
	CompressHook(next CompressionHook) CompressionHook
	DecompressHook(next CompressionHook) CompressionHook
}

type hooksMixin struct {
	hooks   []Hook
	initial hooks
	current hooks
}

// AddHook adds a Hook to the processing chain.
func (hs *hooksMixin) AddHook(hook Hook) {
	hs.hooks = append(hs.hooks, hook)
	hs.chain()
}

func (hs *hooksMixin) initHooks(hooks hooks) {
	hs.initial = hooks
	hs.chain()
}

func (hs *hooksMixin) chain() {
	hs.initial.setDefaults()

	hs.current.marshal = hs.initial.marshal
	hs.current.unmarshall = hs.initial.unmarshall
	hs.current.compress = hs.initial.compress
	hs.current.decompress = hs.initial.decompress

	for i := len(hs.hooks) - 1; i >= 0; i-- {
		if wrapped := hs.hooks[i].MarshalHook(hs.current.marshal); wrapped != nil {
			hs.current.marshal = wrapped
		}
		if wrapped := hs.hooks[i].UnmarshallHook(hs.current.unmarshall); wrapped != nil {
			hs.current.unmarshall = wrapped
		}
		if wrapped := hs.hooks[i].CompressHook(hs.current.compress); wrapped != nil {
			hs.current.compress = wrapped
		}
		if wrapped := hs.hooks[i].DecompressHook(hs.current.decompress); wrapped != nil {
			hs.current.decompress = wrapped
		}
	}
}

type hooks struct {
	marshal    Marshaller
	unmarshall Unmarshaller
	compress   CompressionHook
	decompress CompressionHook
}

func (h *hooks) setDefaults() {
	if h.marshal == nil {
		h.marshal = func(v any) ([]byte, error) {
			return nil, nil
		}
	}
	if h.unmarshall == nil {
		h.unmarshall = func(b []byte, v any) error {
			return nil
		}
	}
	if h.compress == nil {
		h.compress = func(data []byte) ([]byte, error) {
			return nil, nil
		}
	}
	if h.decompress == nil {
		h.decompress = func(data []byte) ([]byte, error) {
			return nil, nil
		}
	}
}
