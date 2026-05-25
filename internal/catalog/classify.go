package catalog

import (
	"regexp"
	"strings"
)

// seasonEpisodeRe matches common season/episode patterns in filenames:
//   - S01E02 (season + episode)
//   - S02    (season only)
//   - E05    (episode only)
var seasonEpisodeRe = regexp.MustCompile(`(?i)\bS\d{1,2}E\d{1,2}\b|\bS\d{1,2}\b|\bE\d{1,3}\b`)

// ClassifyMediaType determines the media type for a file based on tags and
// filename heuristics. The classification priority is:
//  1. Tags: look for "type=movie", "type=series", or "type=anime" (case-insensitive)
//  2. Filename: if the filename contains season/episode patterns, classify as series
//  3. Fallback: return the provided fallback (defaults to movie)
//
// Anime is mapped to series for directory layout purposes.
func ClassifyMediaType(tags []string, filename string, fallback MediaType) MediaType {
	// Step 1: check tags for type=movie, type=series, type=anime
	for _, tag := range tags {
		switch strings.ToLower(tag) {
		case "type=movie":
			return MediaMovie
		case "type=series":
			return MediaSeries
		case "type=anime":
			return MediaSeries // anime goes into series classification
		}
	}

	// Step 2: check filename for season/episode patterns
	if seasonEpisodeRe.MatchString(filename) {
		return MediaSeries
	}

	// Step 3: fallback
	return fallback
}