package marshal

import (
	"testing"
	"fmt"
	"reflect"
)

type Person struct {
	Name    string `csv:"FullName"`
	Gender  string
	Age     int
	Wallet  float32 `csv:"Bank Account"`
	Happy   bool    `true:"Yes!" false:"Sad"`
	private int     `csv:"-"`
}

var (
	sample = []byte(`FullName,Gender,Age,Bank Account,Happy
"Smith, Joe",M,23,19.07,Sad
`)
	person = Person{
		Name: "Smith, Joe",
		Gender: "M",
		Age: 23,
		Wallet: 19.07,
		Happy: false,
	}
)

func TestMarshal(t *testing.T) {
	people := []Person{
		Person{
			Name:   "Smith, Joe",
			Gender: "M",
			Age:    23,
			Wallet: 19.07,
			Happy:  false,
		},
	}

	out, _ := Marshal(people)
	if string(out) != string(sample) {
		t.Error("Marshal result incorrect.")
	}
}

func TestUnmarshal(t *testing.T) {
	people := []Person{}

	err := Unmarshal(sample, &people)

	if err != nil {
		fmt.Println("Error: ", err)
	}

	if !reflect.DeepEqual(people[0], person) {
		t.Error("Unmarshal result incorrect.")
	}
}
