package messaging

import (
	"testing"
	"reflect"
	// "errors"
)


func Test_ParseEventSourcingStruct(t *testing.T) {
	type TestCase struct {
		input []byte
		expectedResult EventSourcingStructure
        expectedError bool
	}

    testCases := []TestCase {
        {
            input: []byte(`{
                "messageUid": "HEJSA",
                "sessionUid": "DAVS",
                "messageBody": "this is a body",
                "senderId": 1124,
                "recipientIds": [88, 1245, 51],
                "fromAutoReply": false,
                "eventDestinations": ["topic1", "topic2"] 
            }`),
            expectedResult: EventSourcingStructure{
                MessageUid: "HEJSA",
                SessionUid: "DAVS",
                MessageBody: "this is a body",
                SendingUserId: 1124,
                RecipientUserIds: []int{88, 1245, 51},
                FromAutoReply: false,
                EventDestinations: []string{"topic1", "topic2"},
            },
            expectedError: false,
        },
        {
            input: []byte(`{}`),
            expectedError: true,
        },
    }

    for _, c := range testCases {

        resultStruct, err := ParseEventSourcingStruct(c.input)

        if err == nil {
            if c.expectedError {
                t.Errorf("ParseEventSourcingStruct: Expected error message but got none.\n")
            }
            
            if !reflect.DeepEqual(c.expectedResult, *resultStruct) {
                t.Errorf("ParseEventSourcingStruct: Expected result, %v but got %v.\n", c.expectedResult, *resultStruct)
            }
        }

    }
}



func Test_PopFirstEventDestination(t *testing.T) {
	type TestCase struct {
		input []string
		expectedHead string
		expectedRemaining []string
		expectedError bool
	}

	testCases := []TestCase {
		{
			input: []string{"1", "2", "3"},
			expectedHead: "1",
			expectedRemaining: []string{"2", "3"},
			expectedError: false,
		},
		{
			input: []string{},
			expectedHead: "",
			expectedRemaining: []string{},
			expectedError: true,
		},
	}

	for _, c := range testCases {

		head, err := PopFirstEventDestination(&c.input)

		if head != c.expectedHead {
			t.Errorf("PopFirstEventDestination: Wrong head.\n")
		}

		if !reflect.DeepEqual(c.input, c.expectedRemaining) {
			t.Errorf("PopFirstEventDestination: Wrong remaining.\n")
		}

		if (err != nil) && !c.expectedError {
			t.Errorf("PopFirstEventDestination: Got an error and should not have.\n")

		} else if (err == nil) && c.expectedError {
			t.Errorf("PopFirstEventDestination: Did not get an error, but should have.\n")
		}

	}
}


func Test_ParseConfigMessage(t *testing.T) {
}



func Test_EncodeEventSourcingStruct(t *testing.T) {
}