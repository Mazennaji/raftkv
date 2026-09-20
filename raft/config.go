package raft

import (
	"os"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	ID      uint64 `yaml:"id"`
	Address string `yaml:"address"`
}

type ClusterConfig struct {
	Nodes []NodeConfig `yaml:"nodes"`
}

func LoadConfig(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg ClusterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
