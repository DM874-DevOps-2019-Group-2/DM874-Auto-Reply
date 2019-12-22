package messaging

import (
	"testing"
	"reflect"
	// "errors"
)


func Test_ParseEventSourcingStruct(t *testing.T) {
}



func Test_PopFirstEventDestination(t *testing.T) {
	type TestCase struct {
		input []string
		expectedHead string
		expectedRemaining []string
		expectedError bool
	}

	cases := []TestCase {
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

	for _, c := range cases {

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