package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile    string
	inputFile  string
	threshold  float64
	columnName string
	jsonOut    bool
	rootCmd    = &cobra.Command{
		Use:   "supercharged",
		Short: "Detect anomalies in a CSV column",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// viper config setup
			if cfgFile != "" {
				viper.SetConfigFile(cfgFile)
				if err := viper.ReadInConfig(); err == nil {
					fmt.Println("Using config file:", viper.ConfigFileUsed())
				}
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			// default action if no subcommand
			cmd.Help()
		},
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.supercharged.yaml)")
	rootCmd.PersistentFlags().StringVarP(&inputFile, "file", "f", "", "CSV file path (required)")
	rootCmd.PersistentFlags().Float64VarP(&threshold, "threshold", "t", 3.0, "Z-score threshold")
	rootCmd.PersistentFlags().StringVarP(&columnName, "column", "c", "", "Column name to analyze (required)")
	rootCmd.PersistentFlags().BoolVarP(&jsonOut, "json", "j", false, "Output results in JSON format")

	viper.BindPFlag("file", rootCmd.PersistentFlags().Lookup("file"))
	viper.BindPFlag("threshold", rootCmd.PersistentFlags().Lookup("threshold"))
	viper.BindPFlag("column", rootCmd.PersistentFlags().Lookup("column"))
	viper.BindPFlag("json", rootCmd.PersistentFlags().Lookup("json"))
}

func initConfig() {
	viper.SetEnvPrefix("SC")
	viper.AutomaticEnv()
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath("$HOME")
		viper.SetConfigName(".supercharged")
	}
}
