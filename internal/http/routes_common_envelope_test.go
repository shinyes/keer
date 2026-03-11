package http

import "testing"

func TestValidatePayloadEnvelope_AcceptsSupportedSlotAlgorithms(t *testing.T) {
	testCases := []struct {
		name      string
		slotType  string
		algorithm string
	}{
		{
			name:      "account master slot",
			slotType:  payloadEnvelopeSlotTypeAccountMaster,
			algorithm: payloadWrapAlgorithmAccountMaster,
		},
		{
			name:      "account public slot",
			slotType:  payloadEnvelopeSlotTypeAccountPublic,
			algorithm: payloadWrapAlgorithmAccountPublic,
		},
		{
			name:      "group key version slot",
			slotType:  payloadEnvelopeSlotTypeGroupKeyVer,
			algorithm: payloadWrapAlgorithmGroupKeyVer,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePayloadEnvelope(&apiPayloadEnvelope{
				WrappedKeys: []apiWrappedKeySlot{
					{
						SlotType:      tc.slotType,
						SlotRef:       "users/1",
						WrapAlgorithm: tc.algorithm,
						WrappedKey:    "wrapped-key",
					},
				},
			})
			if err != nil {
				t.Fatalf("expected envelope to be valid, got err=%v", err)
			}
		})
	}
}

func TestValidatePayloadEnvelope_RejectsMismatchedWrapAlgorithm(t *testing.T) {
	err := validatePayloadEnvelope(&apiPayloadEnvelope{
		WrappedKeys: []apiWrappedKeySlot{
			{
				SlotType:      payloadEnvelopeSlotTypeAccountMaster,
				SlotRef:       "users/1",
				WrapAlgorithm: payloadWrapAlgorithmAccountPublic,
				WrappedKey:    "wrapped-key",
			},
		},
	})
	if err == nil {
		t.Fatalf("expected envelope validation error for mismatched wrap algorithm")
	}
}

func TestValidateGroupKeyVersionWrappedKeys_RejectsNonAccountPublicSlot(t *testing.T) {
	err := validateGroupKeyVersionWrappedKeys([]apiWrappedKeySlot{
		{
			SlotType:      payloadEnvelopeSlotTypeGroupKeyVer,
			SlotRef:       "groups/1/keyVersions/3",
			WrapAlgorithm: payloadWrapAlgorithmGroupKeyVer,
			WrappedKey:    "wrapped-key",
		},
	})
	if err == nil {
		t.Fatalf("expected group key wrappedKeys validation error")
	}
}

func TestValidateGroupKeyVersionWrappedKeys_AcceptsAccountPublicSlot(t *testing.T) {
	err := validateGroupKeyVersionWrappedKeys([]apiWrappedKeySlot{
		{
			SlotType:      payloadEnvelopeSlotTypeAccountPublic,
			SlotRef:       "users/1",
			WrapAlgorithm: payloadWrapAlgorithmAccountPublic,
			WrappedKey:    "wrapped-key",
		},
	})
	if err != nil {
		t.Fatalf("expected group key wrappedKeys to be valid, got err=%v", err)
	}
}
