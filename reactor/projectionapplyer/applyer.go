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
type MultiSaveModelRepo func(ctx context.Context, models map[string]ProjectionModel) errors.Error
type MultiDeleteModelRepo func(ctx context.Context, modelKeys ...*ModelKey) errors.Error
type NewProjectionModel func() ProjectionModel

type ProjectionApplyer struct {
	models          map[string]ProjectionModel
	deleteModelKeys []*ModelKey
	newModel        NewProjectionModel
	getModel        GetModelRepo
	saveModel       MultiSaveModelRepo
	deleteModel     MultiDeleteModelRepo
}

func NewProjectionApplyer(newModel NewProjectionModel, getModel GetModelRepo, saveModel MultiSaveModelRepo, deleteModel MultiDeleteModelRepo) *ProjectionApplyer {
	return &ProjectionApplyer{
		newModel:    newModel,
		getModel:    getModel,
		saveModel:   saveModel,
		deleteModel: deleteModel,
	}
}

func (p *ProjectionApplyer) Clear() {
	p.models = make(map[string]ProjectionModel, 100)
}

func (p *ProjectionApplyer) GetModel(ctx context.Context, modelKey *ModelKey) (ProjectionModel, errors.Error) {
	if p.models == nil {
		p.models = make(map[string]ProjectionModel, 100)
	}

	if mdl, ok := p.models[modelKey.ID]; ok {
		return mdl, nil
	}

	mdl, err := p.getModel(ctx, modelKey)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}

	if errors.IsNotFound(err) {
		mdl = p.newModel()
	}

	p.models[modelKey.ID] = mdl

	return mdl, nil
}

func (p *ProjectionApplyer) ApplyModel(ctx context.Context, modelKey *ModelKey, evt interface{}, msg *reactor.EventMsg) errors.Error {
	mdl, err := p.GetModel(ctx, modelKey)
	if err != nil {
		return err
	}

	return mdl.Apply(evt, msg)
}

func (p *ProjectionApplyer) Save(ctx context.Context) errors.Error {
	for key, mdl := range p.models {
		if !mdl.IsUpdate() || mdl.IsNew() {
			delete(p.models, key)
		}
	}

	if len(p.deleteModelKeys) > 0 {
		if err := p.deleteModel(ctx, p.deleteModelKeys...); err != nil {
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

func (p *ProjectionApplyer) Delete(ctx context.Context, modelKeys ...*ModelKey) {
	for _, modelKey := range modelKeys {
		delete(p.models, modelKey.ID)
	}

	p.deleteModelKeys = append(p.deleteModelKeys, modelKeys...)
}
