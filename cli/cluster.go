package cli

import (
	"log"
	"os"
	"strconv"

	"github.com/abhirockzz/adx-go-create-resources/ops"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var clusterCmd = cobra.Command{Use: "cluster", Short: "work with ADX cluster"}

// ./goadx cluster list --rg <rg name> --sub <sub id>
var listClustersCmd = cobra.Command{Use: "list", Short: "Get ADX clusters in a resource group", Run: listADXClusters}

// ./goadx cluster create --name <cluster name> --loc <location> --rg <rg name> --sub <sub id>
var createClusterCmd = cobra.Command{Use: "create", Short: "Create ADX cluster in a resource group", Long: "Creates 1 instance of compute type DevNoSLAStandardD11V2 in Basic tier", Run: createADXCluster}

// ./goadx cluster delete --name <cluster name> --rg <rg name> --sub <sub id>
var deleteClusterCmd = cobra.Command{Use: "delete", Short: "Deletes an ADX cluster", Run: deleteADXCluster}

func init() {
	clusterCmd.PersistentFlags().String(subscriptionFlag, "", "Azure subscription")
	clusterCmd.PersistentFlags().String(resourceGroupFlag, "", "Azure resource group")
	clusterCmd.MarkPersistentFlagRequired(resourceGroupFlag)
	clusterCmd.MarkPersistentFlagRequired(subscriptionFlag)

	createClusterCmd.Flags().String(nameFlag, "", "ADX cluster name")
	createClusterCmd.Flags().String(locationFlag, "", "ADX cluster location")
	createClusterCmd.MarkFlagRequired(nameFlag)
	createClusterCmd.MarkFlagRequired(locationFlag)

	deleteClusterCmd.Flags().String(nameFlag, "", "ADX cluster name")
	deleteClusterCmd.MarkFlagRequired(nameFlag)

	clusterCmd.AddCommand(&listClustersCmd)
	clusterCmd.AddCommand(&createClusterCmd)
	clusterCmd.AddCommand(&deleteClusterCmd)

	rootCmd.AddCommand(&clusterCmd)
}

func listADXClusters(c *cobra.Command, args []string) {
	rg := c.Flag(resourceGroupFlag).Value.String()
	sub := c.Flag(subscriptionFlag).Value.String()

	log.Println("Listing ADX clusters")

	result := ops.ListClusters(sub, rg)

	data := [][]string{}

	for _, c := range *result.Value {
		data = append(data, []string{*c.Name, string(c.State), *c.Location, strconv.Itoa(int(*c.Sku.Capacity)), *c.URI})
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "State", "Location", "Instances", "URI"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func createADXCluster(c *cobra.Command, args []string) {
	rg := c.Flag(resourceGroupFlag).Value.String()
	sub := c.Flag(subscriptionFlag).Value.String()
	name := c.Flag(nameFlag).Value.String()
	loc := c.Flag(locationFlag).Value.String()

	log.Printf("Creating cluster %s in %s\n", name, loc)

	ops.CreateCluster(sub, name, loc, rg)
}

func deleteADXCluster(c *cobra.Command, args []string) {
	rg := c.Flag(resourceGroupFlag).Value.String()
	sub := c.Flag(subscriptionFlag).Value.String()
	name := c.Flag(nameFlag).Value.String()

	log.Printf("Deleting ADX cluster %s from resource group %s\n", name, rg)

	ops.DeleteCluster(sub, name, rg)
}
