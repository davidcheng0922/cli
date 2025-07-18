package subcmd

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/longhorn/cli/pkg/consts"
	"github.com/longhorn/cli/pkg/remote/preflight"
	"github.com/longhorn/cli/pkg/types"
	"github.com/longhorn/cli/pkg/utils"
)

func NewCmdInstall(globalOpts *types.GlobalCmdOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   consts.SubCmdInstall,
		Short: "Longhorn installation operations",
	}

	utils.SetGlobalOptionsRemote(cmd, globalOpts)

	cmd.AddCommand(newCmdInstallPreflight(globalOpts))

	return cmd
}

func newCmdInstallPreflight(globalOpts *types.GlobalCmdOptions) *cobra.Command {
	var preflightInstaller = preflight.Installer{}

	cmd := &cobra.Command{
		Use:   consts.SubCmdPreflight,
		Short: "Install Longhorn preflight",
		Long: `This command prepares your system for Longhorn deployment by installing the necessary dependencies.
These dependencies ensure your Kubernetes cluster meets the requirements for successful Longhorn operation.

On some OS, like for example SLE Micro, after having installed the needed packages, ` + "`longhornctl`" + ` asks to the user to reboot the machine and
to execute the install command again. During the first execution ` + "`longhornctl`" + ` install needed packages, during the second one it probes modules, start services and configure tools.`,

		Example: `$ longhornctl install preflight
INFO[2024-07-16T17:06:55+08:00] Initializing preflight installer
INFO[2024-07-16T17:06:55+08:00] Cleaning up preflight installer
INFO[2024-07-16T17:06:55+08:00] Running preflight installer
INFO[2024-07-16T17:06:55+08:00] Installing dependencies with package manager
INFO[2024-07-16T17:09:08+08:00] Installed dependencies with package manager
INFO[2024-07-16T17:09:08+08:00] Retrieved preflight installer result:
ip-192-168-208-117:
  info:
  - Successfully installed package nfs-client
  - Successfully installed package open-iscsi
  - Successfully installed package cryptsetup
  - Successfully probed module nfs
  - Successfully probed module iscsi_tcp
  - Successfully probed module dm_crypt
  - Successfully started service iscsid
INFO[2024-07-16T17:09:08+08:00] Cleaning up preflight installer
INFO[2024-07-16T17:09:08+08:00] Completed preflight installer. Use 'longhornctl check preflight' to check the result (on some os a reboot is required first)
` + "```" + `

If a reboot is required, the following message will be displayed:
` + "```" + `
  warn:
  - Need to reboot the system and execute longhornctl install preflight again`,

		PreRun: func(cmd *cobra.Command, args []string) {
			preflightInstaller.Image = globalOpts.Image
			preflightInstaller.KubeConfigPath = globalOpts.KubeConfigPath
			preflightInstaller.NodeSelector = globalOpts.NodeSelector

			logrus.Info("Initializing preflight installer")
			err := preflightInstaller.Init()
			if err != nil {
				utils.CheckErr(errors.Wrap(err, "Failed to initialize preflight installer"))
			}

			logrus.Info("Cleaning up preflight installer")
			if err := preflightInstaller.Cleanup(); err != nil {
				utils.CheckErr(errors.Wrapf(err, "Failed to cleanup preflight installer"))
			}
		},

		Run: func(cmd *cobra.Command, args []string) {
			logrus.Info("Running preflight installer")
			output, err := preflightInstaller.Run()
			if err != nil {
				utils.CheckErr(errors.Wrap(err, "Failed to run preflight installer"))
			}

			logrus.Infof("Retrieved preflight installer result:\n%v", output)
		},

		PostRun: func(cmd *cobra.Command, args []string) {
			logrus.Info("Cleaning up preflight installer")
			if preflightInstaller.OperatingSystem != string(consts.OperatingSystemContainerOptimizedOS) {
				if err := preflightInstaller.Cleanup(); err != nil {
					utils.CheckErr(errors.Wrapf(err, "Failed to cleanup preflight installer"))
				}
			}

			logrus.Infof("Completed preflight installer. Use '%s %s %s' to check the result (on some os a reboot and a new install execution is required first)", consts.CmdLonghornctlRemote, consts.SubCmdCheck, consts.SubCmdPreflight)
		},
	}

	cmd.AddCommand(newCmdInstallPreflightStop(globalOpts))

	utils.SetGlobalOptionsRemote(cmd, globalOpts)

	cmd.Flags().StringVar(&preflightInstaller.OperatingSystem, consts.CmdOptOperatingSystem, "", "Specify the operating system (\"\", cos). Leave this empty to use the package manager for installation.")
	cmd.Flags().BoolVar(&preflightInstaller.UpdatePackages, consts.CmdOptUpdatePackages, true, "Update packages before installing required dependencies.")
	cmd.Flags().BoolVar(&preflightInstaller.EnableSpdk, consts.CmdOptEnableSpdk, false, "Enable installation of SPDK required packages, modules, and setup.")
	cmd.Flags().StringVar(&preflightInstaller.SpdkOptions, consts.CmdOptSpdkOptions, "", fmt.Sprintf("Specify a comma-separated (%s) list of custom options for configuring SPDK environment.", consts.CmdOptSeperator))
	cmd.Flags().IntVar(&preflightInstaller.HugePageSize, consts.CmdOptHugePageSize, 2048, "Specify the huge page size in MiB for SPDK.")
	cmd.Flags().StringVar(&preflightInstaller.AllowPci, consts.CmdOptAllowPci, "none", fmt.Sprintf("Specify a comma-separated (%s) list of allowed PCI devices. By default, all PCI devices are blocked by a non-valid address.", consts.CmdOptSeperator))
	cmd.Flags().StringVar(&preflightInstaller.DriverOverride, consts.CmdOptDriverOverride, "", "Userspace driver for device bindings. Override default driver for PCI devices.")

	return cmd
}

func newCmdInstallPreflightStop(globalOpts *types.GlobalCmdOptions) *cobra.Command {
	var preflightInstaller = preflight.Installer{}

	cmd := &cobra.Command{
		Use:   consts.SubCmdStop,
		Short: "Stop Longhorn preflight installer",
		Long:  `This command terminates the preflight installer.`,
		Example: `$ longhornctl install preflight stop
INFO[2024-07-16T17:21:32+08:00] Stopping preflight installer
INFO[2024-07-16T17:21:32+08:00] Successfully stopped preflight installer`,

		PreRun: func(cmd *cobra.Command, args []string) {
			preflightInstaller.KubeConfigPath = globalOpts.KubeConfigPath

			if err := preflightInstaller.Init(); err != nil {
				utils.CheckErr(errors.Wrap(err, "Failed to initialize preflight installer"))
			}
		},

		Run: func(cmd *cobra.Command, args []string) {
			logrus.Info("Stopping preflight installer")

			err := preflightInstaller.Cleanup()
			if err != nil {
				utils.CheckErr(errors.Wrap(err, "Failed to stop preflight installer"))
			}

			logrus.Info("Successfully stopped preflight installer")
		},
	}

	utils.SetGlobalOptionsRemote(cmd, globalOpts)

	cmd.Flags().StringVar(&preflightInstaller.OperatingSystem, consts.CmdOptOperatingSystem, "", "Specify the operating system (\"\", cos). Leave this empty to use the package manager for installation.")

	// Include flags from the parent command for user convenience. This allows
	// the `stop` subcommand to be appended directly to the `export replica` command
	// without having to remove the irrelevant option flags.	utils.SetFlagHidden(cmd, consts.CmdOptUpdatePackages)
	utils.SetFlagHidden(cmd, consts.CmdOptEnableSpdk)
	utils.SetFlagHidden(cmd, consts.CmdOptSpdkOptions)
	utils.SetFlagHidden(cmd, consts.CmdOptHugePageSize)
	utils.SetFlagHidden(cmd, consts.CmdOptAllowPci)
	utils.SetFlagHidden(cmd, consts.CmdOptDriverOverride)

	return cmd
}
