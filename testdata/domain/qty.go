package domain

import "errors"

type Qty int

var (
	QtyNotBeZero = errors.New("Qty cannot be 0 or negative")
)

func (qty Qty) Add(amount Qty) Qty {
	return qty + amount
}

func (qty Qty) Sub(amount Qty) (Qty, error) {
	newqty := qty - amount
	if newqty < 1 {
		return 0, QtyNotBeZero
	}

	return newqty, nil
}

func (qty Qty) ToInt() int {
	return int(qty)
}
