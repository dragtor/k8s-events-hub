package utils

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type Config struct {
	KafkaBrokerURL  string `mapstructure:"KAFKA_BROKER_URL"`
	FanOutTopicName string `mapstructure:"FAN_OUT_TOPIC_NAME"`
	InClusterConfig string `mapstructure:"INCLUSTER_DEPLOYMENT_ENABLED"`
}

func LoadConfig() (*Config, error) {
	log.Info().Msg("Loading Application Configuration environment ")
	viper.SetConfigFile(".env")
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		log.Error().Err(err)
		return nil, err
	}

	var config Config
	err = viper.Unmarshal(&config)
	if err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal env file")
		return nil, err
	}
	return &config, nil
}
