package db

import (
	"fmt"
	"syscall"
	"time"

	"github.com/semi-technologies/weaviate/entities/storagestate"
)

type diskUse struct {
	total uint64
	avail uint64
}

func (d diskUse) percentUsed() float64 {
	used := d.total - d.avail
	return (float64(used) / float64(d.total)) * 100
}

func (d diskUse) String() string {
	GB := 1024 * 1024 * 1024

	return fmt.Sprintf("total: %.2fGB, avail: %.2fGB, used: %.2fGB",
		float64(d.total)/float64(GB),
		float64(d.avail)/float64(GB),
		float64(d.total-d.avail)/float64(GB))
}

func (s *Shard) scanDiskUse() {
	for {
		time.Sleep(time.Second * 30)

		fs := syscall.Statfs_t{}
		diskPath := s.index.Config.RootPath

		err := syscall.Statfs(diskPath, &fs)
		if err != nil {
			s.index.logger.WithField("action", "read_disk_use").
				WithField("path", diskPath).
				Fatalf("failed to read disk usage: %s", err)
		}

		du := diskUse{
			total: fs.Blocks * uint64(fs.Bsize),
			avail: fs.Bavail * uint64(fs.Bsize),
		}

		s.diskUseWarn(du, diskPath)
		s.diskUseReadonly(du, diskPath)
	}
}

// logs a warning if user-set threshold is surpassed
func (s *Shard) diskUseWarn(du diskUse, diskPath string) {
	if pu := du.percentUsed(); pu > float64(s.index.Config.DiskUseWarningPercentage) {
		if s.index.Config.DiskUseWarningPercentage > 0 {
			s.index.logger.WithField("action", "read_disk_use").
				WithField("shard", s.name).
				WithField("path", diskPath).
				Warnf("disk usage currently at %.2f%%", pu)

			s.index.logger.WithField("action", "disk_use_stats").
				WithField("shard", s.name).
				WithField("path", diskPath).
				Debugf("%s", du.String())
		}
	}
}

// sets the shard to readonly if user-set threshold is surpassed
func (s *Shard) diskUseReadonly(du diskUse, diskPath string) {
	if pu := du.percentUsed(); pu > float64(s.index.Config.DiskUseReadOnlyPercentage) {
		if !s.isReadOnly() && s.index.Config.DiskUseReadOnlyPercentage > 0 {
			err := s.updateStatus(storagestate.StatusReadOnly.String())
			if err != nil {
				s.index.logger.WithField("action", "set_shard_read_only").
					WithField("shard", s.name).
					WithField("path", s.index.Config.RootPath).
					Fatal("failed to set to READONLY")
			}

			s.index.logger.WithField("action", "set_shard_read_only").
				WithField("shard", s.name).
				WithField("path", diskPath).
				Warnf("disk usage currently at %.2f%%, %s set READONLY", pu, s.name)
		}
	}
}
