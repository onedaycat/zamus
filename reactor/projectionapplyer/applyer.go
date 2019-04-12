package projectionapplyer

import (
	"context"

	"github.com/onedaycat/errors"
	"github.com/onedaycat/zamus/eventstore"
	"github.com/onedaycat/zamus/reactor"
)

type ProjectionModel interface {
	IsUpdate() bool
	IsNew() bool
	Apply(evt interface{}, msg *eventstore.EventMsg) errors.Error
}

type ModelKey struct {
	ID       string
	HashKey  string
	RangeKey string
}

func NewModelKey(hashKey string, rangeKey ...string) *ModelKey {
	k := &ModelKey{
		HashKey: hashKey,
	}

	if len(rangeKey) > 0 {
		k.RangeKey = rangeKey[0]
	}

	k.ID = k.HashKey + k.RangeKey

	return k
}

type GetModelRepo func(ctx context.Context, modelKey *ModelKey) (ProjectionModel, errors.Error)
type MultiSaveModelRepo func(ctx context.Context, models []ProjectionModel) errors.Error
type MultiDeleteModelRepo func(ctx context.Context, modelKeys []*ModelKey) errors.Error
type NewProjectionModel func() ProjectionModel

//go:generate mockery -name=ProjectionApplyer
type ProjectionApplyer interface {
	GetModel(ctx context.Context, modelKey *ModelKey) (ProjectionModel, errors.Error)
	ApplyModel(ctx context.Context, modelKey *ModelKey, evt interface{}, msg *reactor.EventMsg) errors.Error
	Save(ctx context.Context) errors.Error
	Delete(ctx context.Context, modelKeys ...*ModelKey)
	Clear()
}

type projectionApplyer struct {
	models              []ProjectionModel
	deleteModelKeys     []*ModelKey
	modelIndex          map[string]int
	deleteModelKeyIndex map[string]int
	newModel            NewProjectionModel
	getModel            GetModelRepo
	saveModel           MultiSaveModelRepo
	deleteModel         MultiDeleteModelRepo
}

func NewProjectionApplyer(newModel NewProjectionModel, getModel GetModelRepo, saveModel MultiSaveModelRepo, deleteModel MultiDeleteModelRepo) *projectionApplyer {
	p := &projectionApplyer{
		newModel:    newModel,
		getModel:    getModel,
		saveModel:   saveModel,
		deleteModel: deleteModel,
	}
	p.Clear()

	return p
}

func (p *projectionApplyer) Clear() {
	p.modelIndex = make(map[string]int, 100)
	p.models = make([]ProjectionModel, 0, 100)
	p.deleteModelKeys = make([]*ModelKey, 0, 100)
	p.deleteModelKeyIndex = make(map[string]int, 100)
}

func (p *projectionApplyer) GetModel(ctx context.Context, modelKey *ModelKey) (ProjectionModel, errors.Error) {
	if i, ok := p.modelIndex[modelKey.ID]; ok {
		return p.models[i], nil
	}

	mdl, err := p.getModel(ctx, modelKey)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}

	if errors.IsNotFound(err) {
		mdl = p.newModel()
	}

	p.models = append(p.models, mdl)
	p.modelIndex[modelKey.ID] = len(p.models) - 1

	return mdl, nil
}

func (p *projectionApplyer) ApplyModel(ctx context.Context, modelKey *ModelKey, evt interface{}, msg *reactor.EventMsg) errors.Error {
	mdl, err := p.GetModel(ctx, modelKey)
	if err != nil {
		return err
	}

	return mdl.Apply(evt, msg)
}

func (p *projectionApplyer) Save(ctx context.Context) errors.Error {
	defer p.Clear()
	var mdl ProjectionModel
	for _, i := range p.modelIndex {
		mdl = p.models[i]
		if !mdl.IsUpdate() || mdl.IsNew() {
			if len(p.models) == 1 {
				p.models = nil
			} else {
				p.models = append(p.models[:i], p.models[i+1:]...)
			}
		}
	}

	if len(p.deleteModelKeys) > 0 {
		if err := p.deleteModel(ctx, p.deleteModelKeys); err != nil {
			return err
		}
	}

	if len(p.models) > 0 {
		if err := p.saveModel(ctx, p.models); err != nil {
			return err
		}
	}

	return nil
}

func (p *projectionApplyer) Delete(ctx context.Context, modelKeys ...*ModelKey) {
	for _, modelKey := range modelKeys {
		i, ok := p.modelIndex[modelKey.ID]
		if ok {
			if len(p.models) == 1 {
				p.models = nil
			} else {
				p.models = append(p.models[:i], p.models[i+1:]...)
			}

			delete(p.modelIndex, modelKey.ID)
		}

		_, ok = p.deleteModelKeyIndex[modelKey.ID]
		if !ok {
			p.deleteModelKeys = append(p.deleteModelKeys, modelKey)
			p.deleteModelKeyIndex[modelKey.ID] = len(p.deleteModelKeys) - 1
		}
	}
}
