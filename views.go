func (ei *ElasticsearchImpl) TotalViewsCreatorStore(InfluencerId string, StartDate string, EndDate string) (*schema.StoreVisitsResp, error) {
	DateFilterTimeFormat, err := ei.DateProcessForFilter(StartDate, EndDate)
	if err != nil {
		ei.Logger.Err(err).Msg("failed to format filter date")
		return nil, errors.Wrap(err, "failed to format filter date")
	}

	var storeVisitAnalytics schema.StoreVisitsResp
	var queries []elastic.Query
	queries = append(queries, elastic.NewTermQuery("creator_id", InfluencerId))
	queries = append(queries, elastic.NewTermsQueryFromStrings("event_action", model.EventActionStoreVisits))
	query := elastic.NewBoolQuery().Must(queries...)
	aggs := elastic.NewDateRangeAggregation().AddRangeWithKey("new_views", DateFilterTimeFormat.NewStartDate, DateFilterTimeFormat.NewEndDate).AddRangeWithKey("old_views", DateFilterTimeFormat.OldStartDate, DateFilterTimeFormat.OldEndDate).Field("timestamp").Keyed(true)

	resp, err := ei.Client.Search().Index(ei.Config.StoreVisitsIndex).Query(query).Aggregation("date_range", aggs).Size(0).Do(context.Background())
	if err != nil {
		ei.Logger.Err(err).Msg("failed to get store views from ELS")
		return nil, errors.Wrap(err, "failed to top get store views")
	}

	var oldViews int
	var newViews int
	if aggs, ok := resp.Aggregations.Terms("date_range"); ok {
		for _, bucket := range aggs.Aggregations {
			var v schema.Views
			err := json.Unmarshal(bucket, &v)
			if err != nil {
				return nil, errors.Wrap(err, "failed to top get store views")
			}
			oldViews = v.OldViews.DocCount
			newViews = v.NewViews.DocCount

		}
	}
	storeVisitAnalytics.StoreVisitsGrowthPercent = ei.PercentageDifferenceBtwTwoValues(oldViews, newViews)
	storeVisitAnalytics.StoreVisits = newViews

	return &storeVisitAnalytics, nil
}

func (ei *ElasticsearchImpl) DateProcessForFilter(newStartDateString string, newEndDateString string) (*schema.DateTimeFilterOpts, error) {
	format := "2006-01-02"
	location, _ := time.LoadLocation("Asia/Kolkata")
	startDateUtc, err := time.ParseInLocation(format, newStartDateString, location)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse time:%s", newStartDateString)
	}
	startDateIST := startDateUtc.Add(5*time.Hour + 30*time.Minute)

	endDateUtc, err := time.ParseInLocation(format, newEndDateString, location)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse time:%s", newEndDateString)
	}
	endDateIST := startDateUtc.Add(5*time.Hour + 30*time.Minute)

	sd := startDateUtc.UTC()
	ed := endDateUtc.UTC()
	d := ed.Sub(sd)
	noOfDays := 1 + int64(d.Hours()/24)
	backStartDate := startDateUtc.AddDate(0, 0, -int(noOfDays))
	backEndDate := endDateUtc.AddDate(0, 0, -int(noOfDays))

	DatesInTimeFormat := schema.DateTimeFilterOpts{
		NewStartDate: startDateIST,
		NewEndDate:   endDateIST,
		OldStartDate: backStartDate,
		OldEndDate:   backEndDate,
	}
	return &DatesInTimeFormat, nil
}

func (ei *ElasticsearchImpl) PercentageDifferenceBtwTwoValues(oldNumber int, newNumber int) float64 {
	if oldNumber == 0 && newNumber == 0 {
		return 0
	}
	if oldNumber == 0 && newNumber != 0 {
		return 100
	}
	diff := float64(newNumber) - float64(oldNumber)
	percentage := diff / float64(oldNumber)
	return percentage * 100
}
