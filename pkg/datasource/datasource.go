package datasource

import (
	"github.com/adlternative/tinygitfs/pkg/data"
	"github.com/adlternative/tinygitfs/pkg/metadata"
)

type DataSource struct {
	Meta *metadata.RedisMeta
	Data *data.MinioData
}
