package torbox

type apiListResponse struct {
	Data []apiDownloadItem `json:"data"`
}

type apiDownloadItem struct {
	ID     float64       `json:"id"`
	Hash   string        `json:"hash"`
	Name   string        `json:"name"`
	Cached bool          `json:"cached"`
	Files  []apiFileItem `json:"files"`
	Tags   interface{}   `json:"tags"`
}

type apiFileItem struct {
	ID        float64 `json:"id"`
	ShortName string  `json:"short_name"`
	Name      string  `json:"name"`
	Size      float64 `json:"size"`
	MimeType  string  `json:"mimetype"`
}