package kinesisstream

import (
	"context"

	"github.com/onedaycat/errors"
)

type PartitionModel interface {
	Apply(msg *EventMsg) errors.Error
}

type PartitionGetModel func(id string) (PartitionModel, errors.Error)
type PartitionSaveModel func(pm PartitionModel) errors.Error
type CreatePartitionModel func() PartitionModel

type partitionHandler struct {
	PartitionModel       PartitionModel
	PartitionGetModel    PartitionGetModel
	PartitionSaveModel   PartitionSaveModel
	CreatePartitionModel CreatePartitionModel
	FilterEvents         []string
}

func NewPartitionHandler(pg PartitionGetModel, ps PartitionSaveModel, cp CreatePartitionModel, fs []string) *partitionHandler {
	return &partitionHandler{
		PartitionGetModel:    pg,
		PartitionSaveModel:   ps,
		CreatePartitionModel: cp,
		FilterEvents:         fs,
	}
}

func (h *partitionHandler) GetFilterEvents() []string {
	return h.FilterEvents
}

func (h *partitionHandler) Apply(ctx context.Context, msgs EventMsgs) errors.Error {
	model, err := h.PartitionGetModel(msgs[0].AggregateID)
	if err != nil && err.GetStatus() != errors.NotFoundStatus {
		return err
	}

	if err.GetStatus() == errors.NotFoundStatus {
		model = h.CreatePartitionModel()
	}

	for _, msg := range msgs {
		if err := model.Apply(msg); err != nil {
			return err
		}
	}

	return h.PartitionSaveModel(model)
}
