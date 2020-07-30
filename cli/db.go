package cli

import (
	"log"
	"os"

	"github.com/abhirockzz/adx-go-create-resources/ops"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

const clusterNameForDBFlag = "cluster"

//const dbNameFlag = "name"

var dbCmd = cobra.Command{Use: "db", Short: "work with ADX database"}

// ./goadx db list --cluster <cluster name> --rg <rg nanme> --sub <sub id>
var listDBsCmd = cobra.Command{Use: "list", Short: "Get databases in an ADX cluster", Run: listDatabases}

// ./goadx db create --name <db name> --cluster <cluster name> --loc <location> --rg <rg name> --sub <sub id>
var createDBCmd = cobra.Command{Use: "create", Short: "Create database in an ADX cluster", Run: createDatabase}

// ./goadx db delete --name <db name> --cluster <cluster name> --rg <rg name> --sub <sub id>
var deleteDBCmd = cobra.Command{Use: "delete", Short: "Delete database from an ADX cluster", Run: deleteDatabase}

func init() {
	dbCmd.PersistentFlags().String(subscriptionFlag, "", "Azure subscription")
	dbCmd.PersistentFlags().String(resourceGroupFlag, "", "Azure resource group")
	dbCmd.PersistentFlags().String(clusterNameForDBFlag, "", "ADX cluster name")
	dbCmd.MarkPersistentFlagRequired(subscriptionFlag)
	dbCmd.MarkPersistentFlagRequired(resourceGroupFlag)
	dbCmd.MarkPersistentFlagRequired(clusterNameForDBFlag)

	createDBCmd.Flags().String(nameFlag, "", "ADX DB name")
	createDBCmd.Flags().String(locationFlag, "", "ADX cluster location")
	createDBCmd.MarkFlagRequired(nameFlag)
	createDBCmd.MarkFlagRequired(locationFlag)

	deleteDBCmd.Flags().String(nameFlag, "", "ADX DB name")
	deleteDBCmd.MarkFlagRequired(nameFlag)

	dbCmd.AddCommand(&listDBsCmd)
	dbCmd.AddCommand(&createDBCmd)
	dbCmd.AddCommand(&deleteDBCmd)

	rootCmd.AddCommand(&dbCmd)
}

func listDatabases(c *cobra.Command, args []string) {
	rg := c.Flag(resourceGroupFlag).Value.String()
	sub := c.Flag(subscriptionFlag).Value.String()
	cluster := c.Flag(clusterNameForDBFlag).Value.String()

	log.Printf("Listing databases in ADX cluster %s\n", cluster)

	result := ops.ListDBs(sub, rg, cluster)

	data := [][]string{}

	for _, db := range *result.Value {
		rwDB, isRW := db.AsReadWriteDatabase()
		if isRW {
			data = append(data, []string{*rwDB.Name, string(rwDB.ProvisioningState), *rwDB.Location, *rwDB.Type})
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "State", "Location", "Type"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func createDatabase(c *cobra.Command, args []string) {
	rg := c.Flag(resourceGroupFlag).Value.String()
	sub := c.Flag(subscriptionFlag).Value.String()
	cluster := c.Flag(clusterNameForDBFlag).Value.String()
	dbName := c.Flag(nameFlag).Value.String()
	loc := c.Flag(locationFlag).Value.String()

	log.Printf("Creating DB in ADX cluster %s in %s\n", dbName, cluster)

	ops.CreateDatabase(sub, rg, cluster, loc, dbName)
}

func deleteDatabase(c *cobra.Command, args []string) {
	rg := c.Flag(resourceGroupFlag).Value.String()
	sub := c.Flag(subscriptionFlag).Value.String()
	cluster := c.Flag(clusterNameForDBFlag).Value.String()
	dbName := c.Flag(nameFlag).Value.String()

	log.Printf("Deleting DB from ADX cluster %s in %s\n", dbName, cluster)

	ops.DeleteDB(sub, rg, cluster, dbName)
}
