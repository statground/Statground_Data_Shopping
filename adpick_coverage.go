package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type adpickQueryCoverage struct {
	Vertical      string                   `json:"vertical"`
	Category      string                   `json:"category"`
	Keyword       string                   `json:"keyword"`
	Returned      int                      `json:"returned"`
	NewOffers     int                      `json:"new_offers"`
	Duplicates    int                      `json:"duplicates"`
	Excluded      int                      `json:"excluded_merchant_or_vertical"`
	Invalid       int                      `json:"invalid_title_or_link"`
	MerchantCodes map[string]int           `json:"returned_merchant_codes"`
	Samples       []adpickDiagnosticSample `json:"samples,omitempty"`
}

type adpickMerchantCoverage struct {
	Code     string `json:"cp_code"`
	Vertical string `json:"vertical"`
	Offers   int    `json:"offers"`
}

type adpickCoverage struct {
	RunUUID             string                            `json:"run_uuid"`
	StartedAt           string                            `json:"started_at"`
	UpdatedAt           string                            `json:"updated_at"`
	QueriesPlanned      int                               `json:"queries_planned"`
	QueriesCompleted    int                               `json:"queries_completed"`
	Requests            int                               `json:"request_attempts"`
	Retries             int                               `json:"retries"`
	CollectionComplete  bool                              `json:"collection_complete"`
	DiscoveryComplete   bool                              `json:"discovery_complete"`
	RetainedOffers      int                               `json:"retained_previous_offers"`
	EvictedOffers       int                               `json:"evicted_oldest_offers"`
	PublicationComplete bool                              `json:"publication_complete"`
	TargetMet           bool                              `json:"coverage_target_met"`
	Failure             string                            `json:"failure,omitempty"`
	TargetOffers        map[string]int                    `json:"target_offers"`
	OffersByVertical    map[string]int                    `json:"offers_by_vertical"`
	OffersByCategory    map[string]int                    `json:"offers_by_category"`
	Merchants           map[string]adpickMerchantCoverage `json:"merchants"`
	Queries             []adpickQueryCoverage             `json:"queries"`
}

func newAdpickCoverage(planned int) *adpickCoverage {
	return &adpickCoverage{StartedAt: time.Now().UTC().Format(time.RFC3339), QueriesPlanned: planned,
		TargetOffers:     map[string]int{"travel": 200, "services": 100},
		OffersByVertical: map[string]int{}, OffersByCategory: map[string]int{}, Merchants: map[string]adpickMerchantCoverage{}, Queries: []adpickQueryCoverage{}}
}

func writeAdpickCoverage(report *adpickCoverage, client *adpickClient) error {
	report.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	report.Requests, report.Retries = client.requests, client.retries
	report.TargetMet = true
	for vertical, target := range report.TargetOffers {
		if report.OffersByVertical[vertical] < target {
			report.TargetMet = false
		}
	}
	path := envString("ADPICK_COVERAGE_REPORT", "")
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("Adpick coverage encoding failed")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("Adpick coverage directory unavailable")
	}
	// Only counters, controlled query text and validated mall IDs enter this file.
	// Original provider payloads, API errors, URLs and credentials never enter it.
	temp, err := os.CreateTemp(filepath.Dir(path), ".adpick-coverage-*")
	if err != nil {
		return fmt.Errorf("Adpick coverage file unavailable")
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if _, err = temp.Write(append(data, '\n')); err == nil {
		err = temp.Sync()
	}
	closeErr := temp.Close()
	if err != nil || closeErr != nil {
		return fmt.Errorf("Adpick coverage write failed")
	}
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("Adpick coverage replace failed")
	}
	return nil
}

// Search has no documented pagination or merchant filter. Broaden actual search
// phrases while continuing to accept only cp_codes verified by /malls.
// Thirty phrases per category keep each complete run within 240 searches.
func expandedAdpickQueries() []adpickQuery {
	groups := []struct{ vertical, category, keywords string }{
		{"travel", "stays", `트립닷컴 호텔|야놀자 호텔|마이리얼트립 호텔|KKday 호텔|클룩 호텔|트래블로카 호텔|호텔스닷컴 호텔|제주 호텔|서울 호텔|부산 호텔|강릉 호텔|속초 호텔|경주 호텔|여수 호텔|인천 호텔|제주 리조트|제주 펜션|서울 호캉스|오사카 호텔|도쿄 호텔|후쿠오카 호텔|삿포로 호텔|방콕 호텔|다낭 호텔|나트랑 호텔|발리 호텔|타이베이 호텔|싱가포르 호텔|홍콩 호텔|파리 호텔`},
		{"travel", "flights", `항공권|국내 항공권|국제 항공권|제주 항공권|부산 항공권|일본 항공권|오사카 항공권|도쿄 항공권|후쿠오카 항공권|삿포로 항공권|오키나와 항공권|방콕 항공권|다낭 항공권|나트랑 항공권|하노이 항공권|호치민 항공권|발리 항공권|타이베이 항공권|싱가포르 항공권|홍콩 항공권|마닐라 항공권|세부 항공권|보라카이 항공권|괌 항공권|사이판 항공권|하와이 항공권|유럽 항공권|미국 항공권|트립닷컴 항공권|트래블로카 항공권`},
		{"travel", "experiences", `클룩 입장권|KKday 입장권|마이리얼트립 투어|제주 입장권|제주 투어|서울 입장권|부산 투어|경주 투어|오사카 입장권|도쿄 입장권|유니버설 스튜디오 재팬|도쿄 디즈니랜드|홍콩 디즈니랜드|상하이 디즈니랜드|싱가포르 유니버설|대만 투어|방콕 투어|다낭 투어|나트랑 투어|발리 투어|세부 투어|푸켓 투어|보홀 투어|파리 투어|런던 투어|로마 투어|바르셀로나 투어|교토 투어|후쿠오카 투어|삿포로 투어`},
		{"travel", "packages", `패키지 여행|해외 패키지 여행|국내 패키지 여행|제주 여행 패키지|일본 여행 패키지|오사카 여행 패키지|도쿄 여행 패키지|후쿠오카 여행 패키지|삿포로 여행 패키지|오키나와 여행 패키지|베트남 여행 패키지|다낭 여행 패키지|나트랑 여행 패키지|하노이 여행 패키지|태국 여행 패키지|방콕 여행 패키지|푸켓 여행 패키지|발리 여행 패키지|대만 여행 패키지|싱가포르 여행 패키지|홍콩 여행 패키지|세부 여행 패키지|보홀 여행 패키지|괌 여행 패키지|하와이 여행 패키지|유럽 여행 패키지|스위스 여행 패키지|이탈리아 여행 패키지|스페인 여행 패키지|마이리얼트립 패키지`},
		{"services", "design", `크몽 디자인|로고 디자인|브랜드 디자인|명함 디자인|상세페이지 디자인|배너 디자인|썸네일 디자인|포스터 디자인|전단지 디자인|브로슈어 디자인|카탈로그 디자인|패키지 디자인|인포그래픽 디자인|PPT 디자인|제안서 디자인|카드뉴스 디자인|캐릭터 디자인|일러스트 제작|웹디자인|앱 디자인|UI UX 디자인|피그마 디자인|간판 디자인|메뉴판 디자인|스티커 디자인|이모티콘 제작|제품 디자인|3D 모델링|크몽 로고|크몽 상세페이지`},
		{"services", "development", `웹사이트 제작|홈페이지 제작|쇼핑몰 제작|랜딩페이지 제작|기업 홈페이지|반응형 웹 제작|워드프레스 제작|아임웹 제작|카페24 제작|웹 개발|앱 개발|안드로이드 개발|아이폰 앱 개발|플러터 개발|리액트 개발|프론트엔드 개발|백엔드 개발|웹 퍼블리싱|API 개발|데이터베이스 개발|업무 자동화|엑셀 자동화|파이썬 자동화|챗봇 개발|크롤링 개발|프로그램 개발|웹사이트 유지보수|서버 구축|크몽 개발|크몽 홈페이지`},
		{"services", "marketing", `광고 마케팅|검색 광고|SNS 마케팅|인스타그램 마케팅|유튜브 마케팅|블로그 마케팅|콘텐츠 마케팅|브랜드 마케팅|퍼포먼스 마케팅|네이버 광고|구글 광고|메타 광고|카카오 광고|광고 운영 대행|온라인 홍보|홍보 영상 제작|숏폼 영상 제작|영상 편집|광고 영상|제품 촬영|상품 사진 촬영|브랜드 컨설팅|마케팅 컨설팅|시장 조사|키워드 분석|SEO 최적화|검색엔진 최적화|보도자료 작성|크몽 마케팅|크몽 광고`},
		{"services", "writing", `번역|영어 번역|일본어 번역|중국어 번역|독일어 번역|프랑스어 번역|스페인어 번역|베트남어 번역|태국어 번역|전문 번역|문서 번역|영상 자막 번역|영문 교정|영문 교열|한국어 교정|교정 교열|원고 교정|글쓰기 대행|카피라이팅|브랜드 스토리|제품 소개글|회사 소개서|사업계획서 작성|제안서 작성|매뉴얼 작성|인터뷰 원고|자막 제작|녹취록 작성|크몽 번역|크몽 카피라이팅`},
	}
	queries := make([]adpickQuery, 0, adpickMaximumQueries)
	// Round-robin categories exposes source coverage early even on a later failure.
	for i := 0; i < 30; i++ {
		for _, group := range groups {
			keywords := strings.Split(group.keywords, "|")
			queries = append(queries, adpickQuery{group.vertical, group.category, keywords[i]})
		}
	}
	return queries
}
