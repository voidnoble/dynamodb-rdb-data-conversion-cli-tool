package postgres

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// convGroups is conversion group
// 미완성이라 ConvGroups 로 export 하지 않음
func convGroups(ddbGroups []Group) (groups []Group, err error) {
	for i, item := range ddbGroups {
		fmt.Printf("%d", i)

		// Photo item
		isPhotoStartWithHTTP, errPhotoStartWithHTTP := regexp.MatchString("^http", item.Photo)
		isPhotoStartWithHTTPFileDomain, errPhotoStartWithHTTPFileDomain := regexp.MatchString("^http://file.example.com", item.Photo)
		isPhotoContainSpecificWords, errPhotoContainSpecificWords := regexp.MatchString("family/|user/|group/|team/|cloudfront|google", item.Photo)

		if item.Photo == "" {
			item.Photo = ""
		} else if isPhotoStartWithHTTPFileDomain && errPhotoStartWithHTTP == nil {
			item.Photo = strings.Replace(item.Photo, "http://file.example.com", "https://images.example.co", -1)
		} else if !isPhotoStartWithHTTP && isPhotoContainSpecificWords && errPhotoStartWithHTTPFileDomain == nil && errPhotoContainSpecificWords == nil {
			item.Photo = "https://images.example.co/" + item.Photo // 값 앞에 이미지 url prefix 붙임
		}

		// Name item
		if len(item.Name) > 100 {
			item.Name = item.Name[:100]
		}

		// SportsType item
		if item.Sports == "soccer" || item.Sports == "축구" {
			item.SportsType = 0
		} else {
			item.SportsType = 100
		}

		// Age item
		if item.Agegroup == "adult" {
			item.Age = 0
		} else if item.Agegroup == "college" {
			item.Age = 1
		}

		// IsDeleted item
		if item.Deleted {
			item.IsDeleted = 1
		} else {
			item.IsDeleted = 0
		}

		// IsPrivate item
		if item.IsPublic {
			item.IsPrivate = 0
		} else {
			item.IsPrivate = 1
		}

		// IsPrivate item
		if item.CreatedAt == 0 {
			item.OnCreated = "1970-01-01 00:00:00"
		} else {
			createdAt := item.CreatedAt / 1000
			createdDateTime := time.Unix(createdAt, 0)
			item.OnCreated = createdDateTime.Format("2006-01-02 15:04:05") //createdDateTime.String()[:20]
		}

		// Append to groups array
		groups = append(groups, item)
	}

	// groups = ddbGroups

	return groups, err
}
