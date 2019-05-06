package sagacmd

type StateDefinitions struct {
    Definitions []*StateDefinition
}

func (s *StateDefinitions) GetState(name string) *StateDefinition {
    for _, def := range s.Definitions {
        if def.Name == name {
            return def
        }
    }

    return nil
}

type StateDefinition struct {
    Name              string
    StepHandler       StepHandler
    CompensateHandler CompensateHandler
}
