package daos

import (
	"github.com/arangodb/go-driver"
	"github.com/gitalytics/api/app"
	"github.com/gitalytics/api/models"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"strconv"
	"strings"
	"time"
)

const (
	CommitCollection = "commit"
)

// CommitDAO persists commit data in database
type CommitDAO struct{}

// NewCommitDAO creates a new CommitDAO
func NewCommitDAO() *CommitDAO {
	return &CommitDAO{}
}

func (dao *CommitDAO) GetCommitsAggregatedStats(rs app.RequestScope, bindings map[string]interface{}) ([]*models.CommitAggregation, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	groupBy := dao.getGroupBy(rs, &bindings, "group_by")

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT bucket = c." + groupBy + " INTO cms = c " +
		"LET commits = FLATTEN(cms[*].stats) " +
		"LET languages = FLATTEN(commits[*].languages) " +
		workTypesVars +
		"SORT bucket " +
		"RETURN {" +
		"_key: TO_STRING(bucket), " +
		"count: LENGTH(cms), " +
		"impact: SUM(cms[*].impact), " +
		"work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}" +
		"}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	commitAggregations := make([]*models.CommitAggregation, cursor.Count())
	for {
		var item *models.CommitAggregation
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		commitAggregations = append(commitAggregations, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCommitsAggregatedStats AQL timer [%s]", time.Since(start))

	return commitAggregations, nil
}

// GetCommitsPunchCard returns either 1 or 0 for each day withing the specified date range
// Result is set based on the distance between the current date bucket and the next one
// If distance is 1, so is the result value. If distance is grater than 1, the result is 0
func (dao *CommitDAO) GetCommitsPunchCard(rs app.RequestScope, bindings map[string]interface{}) ([]map[string]interface{}, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "LET result = (FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT bucket = c." + dao.getGroupByField(rs, "year_month_day") + " " +
		"SORT bucket DESC " +
		"RETURN TO_STRING(bucket)) " +

		"LET length = LENGTH(result) - 1 " +
		"FOR i IN 1..length " +
		"LET diff = (result[i] == null ? 0 : DATE_DIFF(result[i], result[i-1], \"day\")) " +
		"RETURN {diff: diff, bucket: result[i-1]}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []map[string]interface{}
	for {
		var item map[string]interface{}
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCommitsPunchCard AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetContributorsDaysWorked(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorDaysWorked, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT contributor_id = c.contributor_id, bucket_id = c." + dao.getGroupByField(rs, "year_month_day") + " " +
		"RETURN {contributor_id: contributor_id, bucket_id: TO_STRING(bucket_id)}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.ContributorDaysWorked
	for {
		var item *models.ContributorDaysWorked
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorsDaysWorked AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetCommitsTimeFidelityAggregatedStats(rs app.RequestScope, bindings map[string]interface{}) ([]*models.CommitTimeFidelityAggregation, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	groupBy := dao.getGroupBy(rs, &bindings, "group_by")
	secondaryGroupBy := dao.getGroupBy(rs, &bindings, "secondary_group_by")

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT bucket = c." + groupBy + ", secondary_bucket = c." + secondaryGroupBy + " INTO commits = c.stats " +
		"LET languages = FLATTEN(commits[*].languages) " +
		workTypesVars +
		"SORT bucket, secondary_bucket " +
		"RETURN {" +
		"_key: TO_STRING(bucket), " +
		"bucket_id: TO_STRING(secondary_bucket), " +
		"count: LENGTH(commits), " +
		"work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}" +
		"}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	commitTimeFidelityAggregations := make([]*models.CommitTimeFidelityAggregation, cursor.Count())
	for {
		var item *models.CommitTimeFidelityAggregation
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		commitTimeFidelityAggregations = append(commitTimeFidelityAggregations, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCommitsTimeFidelityAggregatedStats AQL timer [%s]", time.Since(start))

	return commitTimeFidelityAggregations, nil
}

func (dao *CommitDAO) GetContributorsWorkHours(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorDayWorkHours, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT contributor_id = c.contributor_id, day = c." + dao.getGroupByField(rs, "year_month_day") + ", hour = c." + dao.getGroupByField(rs, "hour") + " " +
		"RETURN {contributor_id: contributor_id, day: TO_STRING(day), hour: TO_STRING(hour)}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.ContributorDayWorkHours
	for {
		var item *models.ContributorDayWorkHours
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorsWorkHours AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetContributorsCommitCount(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorCommitCount, error) {
	start := time.Now().UTC()

	contributorFilter := ""
	if _, ok := bindings["contributor_id"].([]string); ok == true {
		contributorFilter = "FILTER ctr._key IN @contributor_id "
	}

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.addFilter("c.contributor_id == ctr._key")
	queryBuilder.buildFilters(bindings)

	sortDirection := dao.getSortDirection(&bindings)

	query := "FOR ctr IN contributor " +
		contributorFilter +
		"LET count = LENGTH(FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"RETURN c._key) " +
		"FILTER count > 0 " +
		"SORT count " + sortDirection + " " +
		"RETURN {contributor_id: ctr._key, count: count}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	result := make([]*models.ContributorCommitCount, cursor.Count())
	for {
		var item *models.ContributorCommitCount
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorsCommitCount AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetContributorsCommitTotalCount(rs app.RequestScope, bindings map[string]interface{}) (int, error) {
	start := time.Now().UTC()

	contributorFilter := ""
	if _, ok := bindings["contributor_id"].([]string); ok == true {
		contributorFilter = "FILTER ctr._key IN @contributor_id "
	}

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.addFilter("c.contributor_id == ctr._key")
	queryBuilder.buildFilters(bindings)

	query := "RETURN LENGTH(FOR ctr IN contributor " +
		contributorFilter +
		"FILTER LENGTH(FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"RETURN c._key) > 0 RETURN ctr._key)"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return 0, err
	}
	defer cursor.Close()

	var count int
	for {
		_, err := cursor.ReadDocument(ctx, &count)

		if driver.IsNoMoreDocuments(err) {
			break
		}
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorsCommitTotalCount AQL timer [%s]", time.Since(start))

	return count, nil
}

func (dao *CommitDAO) GetContributorsCommitsAggregatedStats(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorCommitAggregation, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)
	linesOfCodeVars := dao.buildLinesOfCodeVars(bindings)

	groupByCommit := false
	if v, ok := bindings["group_by"].(string); ok == true && v == "timestamp" {
		groupByCommit = true
	}

	groupBy := dao.getGroupBy(rs, &bindings, "group_by")

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	var query string

	// Query needs to be different if grouping by commit's timestamp
	if groupByCommit {
		query = "FOR c IN " + CommitCollection +
			queryBuilder.getFiltersString() +
			"COLLECT contributor_id = c.contributor_id, bucket = c._key INTO commits = {stats: c.stats, date: c.date, impact: c.impact} " +
			"FOR ctr IN contributor FILTER ctr._key == contributor_id " +
			"LET commit = commits[0] " +
			"LET languages = FLATTEN(commit.stats.languages) " +
			workTypesVars +
			linesOfCodeVars +
			"SORT contributor_id, commit.date " +
			"RETURN {" +
			"contributor: ctr, " +
			"_key: TO_STRING(commit.date), " +
			"count: LENGTH(commits), " +
			"work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}, " +
			"loc: loc, " +
			"bucket_id: bucket, " +
			"impact: commit.impact" +
			"}"
	} else {
		query = "FOR c IN " + CommitCollection +
			queryBuilder.getFiltersString() +
			"COLLECT contributor_id = c.contributor_id, bucket = c." + groupBy + " INTO cms = c " +
			"FOR ctr IN contributor FILTER ctr._key == contributor_id " +
			"LET commits = FLATTEN(cms[*].stats) " +
			"LET languages = FLATTEN(commits[*].languages) " +
			workTypesVars +
			linesOfCodeVars +
			"SORT contributor_id, bucket " +
			"RETURN {" +
			"contributor: ctr, " +
			"_key: TO_STRING(bucket), " +
			"count: LENGTH(cms), " +
			"loc: loc, work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}, " +
			"impact: SUM(cms[*].impact)" +
			"}"
	}

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	result := make([]*models.ContributorCommitAggregation, cursor.Count())
	for {
		var item *models.ContributorCommitAggregation
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorsCommitsAggregatedStats AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetTeamsCommitCount(rs app.RequestScope, bindings map[string]interface{}) ([]*models.TeamCommitCount, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.addFilter("c.contributor_id IN contributors")
	queryBuilder.buildFilters(bindings)

	sortDirection := dao.getSortDirection(&bindings)

	query := "FOR t IN team " +
		"LET contributors = (FOR ctr IN 1 OUTBOUND t._id team_contributors RETURN ctr._key) " +
		"LET count = LENGTH(FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"RETURN c._key) " +
		"FILTER count > 0 " +
		"SORT count " + sortDirection + " " +
		"LIMIT @offset, @limit " +
		"RETURN {team: t, count: count}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	result := make([]*models.TeamCommitCount, cursor.Count())
	for {
		var item *models.TeamCommitCount
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetTeamsCommitCount AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetTeamsCommitTotalCount(rs app.RequestScope, bindings map[string]interface{}) (int, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.addFilter("c.contributor_id IN contributors")
	queryBuilder.buildFilters(bindings)

	query := "FOR t IN team " +
		"LET contributors = (FOR ctr IN 1 OUTBOUND t._id team_contributors RETURN ctr._key) " +
		"FILTER LENGTH(FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"RETURN c._key) > 0 " +
		"COLLECT WITH COUNT INTO length RETURN length"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return 0, err
	}
	defer cursor.Close()

	var count int
	for {
		_, err := cursor.ReadDocument(ctx, &count)

		if driver.IsNoMoreDocuments(err) {
			break
		}
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetTeamsCommitTotalCount AQL timer [%s]", time.Since(start))

	return count, nil
}

func (dao *CommitDAO) GetTeamsCommitsAggregatedStats(rs app.RequestScope, bindings map[string]interface{}) ([]*models.TeamCommitAggregation, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)
	linesOfCodeVars := dao.buildLinesOfCodeVars(bindings)

	groupBy := dao.getGroupBy(rs, &bindings, "group_by")

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection + " " +
		"FOR t IN 1 INBOUND CONCAT(\"contributor/\", c.contributor_id) team_contributors " +

		queryBuilder.getFiltersString() +
		"COLLECT team_id = t._key, bucket = c." + groupBy + " INTO cms = c " +
		"LET commits = FLATTEN(cms[*].stats) " +
		"LET languages = FLATTEN(commits[*].languages) " +
		workTypesVars +
		linesOfCodeVars +
		"SORT team_id, bucket " +
		"RETURN {" +
		"team_id: team_id, " +
		"_key: TO_STRING(bucket), " +
		"count: LENGTH(cms), " +
		"work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}, " +
		"loc: loc, " +
		"impact: SUM(cms[*].impact)" +
		"}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	result := make([]*models.TeamCommitAggregation, cursor.Count())
	for {
		var item *models.TeamCommitAggregation
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetTeamsCommitsAggregatedStats AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetContributorsWorkTypesTotalCount(rs app.RequestScope, bindings map[string]interface{}) (int, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "RETURN LENGTH(FOR c IN " + CommitCollection + " " +
		queryBuilder.getFiltersString() +
		"COLLECT contributor_id = c.contributor_id " +
		"RETURN contributor_id)"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return 0, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return 0, err
	}
	defer cursor.Close()

	var count int
	for {
		_, err := cursor.ReadDocument(ctx, &count)

		if driver.IsNoMoreDocuments(err) {
			break
		}
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorsWorkTypesTotalCount AQL timer [%s]", time.Since(start))

	return count, nil
}

func (dao *CommitDAO) GetContributorsWorkTypesAggregatedStats(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorWorkTypesAggregation, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	sort := dao.getWorkTypeSort(&bindings)
	sortDirection := dao.getSortDirection(&bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT contributor_id = c.contributor_id INTO commits = c.stats " +
		"LET languages = FLATTEN(commits[*].languages) " +
		"FOR ctr IN " + CONTRIBUTOR_COLLECTION + " FILTER ctr._key == contributor_id " +
		workTypesVars +
		"LET loc = new_work + refactor + self_churn + others_churn " +
		"LET efficiency = FLOOR((new_work + refactor) * 100 / loc) " +
		"SORT " + sort + " " + sortDirection + ", loc " + sortDirection + " " +
		"LIMIT @offset, @limit " +
		"RETURN {" +
		"contributor: {_key: ctr._key, email: ctr.email, first_name: ctr.first_name, last_name: ctr.last_name}, " +
		"work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}, " +
		"efficiency: efficiency " +
		"}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	result := make([]*models.ContributorWorkTypesAggregation, cursor.Count())
	for {
		var item *models.ContributorWorkTypesAggregation
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorsWorkTypesAggregatedStats AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetContributorsImpactAggregatedStats(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorImpactAggregation, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT contributor_id = c.contributor_id INTO commits = c " +
		"FOR ctr IN " + CONTRIBUTOR_COLLECTION + " FILTER ctr._key == contributor_id " +
		"RETURN {" +
		"contributor: {_key: ctr._key, email: ctr.email, first_name: ctr.first_name, last_name: ctr.last_name}, " +
		"impact: SUM(commits[*].impact) " +
		"}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	result := make([]*models.ContributorImpactAggregation, cursor.Count())
	for {
		var item *models.ContributorImpactAggregation
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorsImpactAggregatedStats AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetLanguageAggregatedStats(rs app.RequestScope, bindings map[string]interface{}) ([]*models.LanguageCommitAggregation, error) {
	start := time.Now().UTC()

	languageLinesOfCodeAggregations := dao.buildLanguageLinesOfCodeAggregations(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	groupBy := dao.getGroupBy(rs, &bindings, "group_by")

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.stats.languages != null " +
		"FOR l IN c.stats.languages " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT bucket = c." + groupBy + ", type = l.type AGGREGATE " + languageLinesOfCodeAggregations +
		"SORT bucket " +
		"RETURN {" +
		"_key: TO_STRING(bucket), " +
		"type: type, " +
		"loc: (additions + modifications - deletions)" +
		"}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var aggregations []*models.LanguageCommitAggregation
	for {
		var item *models.LanguageCommitAggregation
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		aggregations = append(aggregations, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetLanguageAggregatedStats AQL timer [%s]", time.Since(start))

	return aggregations, nil
}

func (dao *CommitDAO) GetTopLanguages(rs app.RequestScope, bindings map[string]interface{}) ([]string, error) {
	start := time.Now().UTC()

	languageLinesOfCodeAggregations := dao.buildLanguageLinesOfCodeAggregations(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.stats.languages != null " +
		"FOR l IN c.stats.languages " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT type = l.type AGGREGATE " + languageLinesOfCodeAggregations +
		"LET loc = (additions + modifications - deletions) " +
		"SORT loc DESC " +
		"LIMIT @offset, @limit " +
		"RETURN type"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var languages []string
	for {
		var language string
		_, err := cursor.ReadDocument(ctx, &language)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		languages = append(languages, language)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetTopLanguages AQL timer [%s]", time.Since(start))

	return languages, nil
}

func (dao *CommitDAO) GetFileCountByLanguage(rs app.RequestScope, bindings map[string]interface{}) ([]*models.LanguageFileCount, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.files != null " +
		"FOR fl IN c.files " +
		dao.buildFileLanguagesFilter(bindings) +
		"COLLECT language = fl.type INTO file_ids = fl._key " +
		"LET count = LENGTH(FOR id IN file_ids COLLECT file_id = id RETURN file_id) " +
		"RETURN {language, count}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.LanguageFileCount
	for {
		var item *models.LanguageFileCount
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetFileCountByLanguage AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetLanguageLinesOfCode(rs app.RequestScope, bindings map[string]interface{}) ([]*models.LanguageLinesOfCode, error) {
	start := time.Now().UTC()

	linesOfCodeSum := dao.buildLOCSum(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "LET files = (FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.files != null " +
		"SORT c.date DESC " +
		"FOR fl IN c.files " +
		"FILTER fl.loc != null " +
		dao.buildFileLanguagesFilter(bindings) +
		"COLLECT language = fl.type, file_id = fl._key INTO locs = fl.loc " +
		"LET loc = locs[0] " +
		"RETURN {language: language, loc: " + linesOfCodeSum + "}) " +
		"FOR fl IN files " +
		"COLLECT language = fl.language AGGREGATE loc = SUM(fl.loc) " +
		"RETURN {language, loc}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.LanguageLinesOfCode
	for {
		var item *models.LanguageLinesOfCode
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetLanguageLinesOfCode AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetLanguageContributors(rs app.RequestScope, bindings map[string]interface{}) ([]*models.LanguageContributors, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.stats.languages != null " +
		"FOR l IN c.stats.languages " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT language = l.type INTO commits = c " +
		"LET contributors = (FOR c IN commits COLLECT contributor_id = c.contributor_id WITH COUNT INTO count " +
		"FOR ctr IN contributor FILTER ctr._key == contributor_id " +
		"SORT count DESC RETURN {_key: ctr._key, last_name: ctr.last_name, first_name: ctr.first_name}) " +
		"RETURN {language: language, count: LENGTH(contributors), contributors: SLICE(contributors, 0, 3)}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.LanguageContributors
	for {
		var item *models.LanguageContributors
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetLanguageContributors AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetFileActivitySummary(rs app.RequestScope, bindings map[string]interface{}) (*models.FilesActivitySummary, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "LET count = LENGTH(FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.files != null " +
		"FOR fl IN c.files " +
		dao.buildFileLanguagesFilter(bindings) +
		"RETURN DISTINCT fl._key) " +
		"RETURN {count: count}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var filesActivitySummary *models.FilesActivitySummary
	for {
		_, err := cursor.ReadDocument(ctx, &filesActivitySummary)
		if err != nil {
			return nil, err
		}
		break
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetFileActivitySummary AQL timer [%s]", time.Since(start))

	return filesActivitySummary, nil
}

func (dao *CommitDAO) GetLanguagesEfficiency(rs app.RequestScope, bindings map[string]interface{}) ([]*models.LanguageEfficiency, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.files != null " +
		"FOR fl IN c.stats.languages " +
		dao.buildFileLanguagesFilter(bindings) +
		"COLLECT type = fl.type INTO commits = c.stats.languages[* FILTER CURRENT.type == fl.type] " +
		"LET languages = FLATTEN(commits[*]) " +
		workTypesVars +
		"LET loc = new_work + refactor + self_churn + others_churn " +
		"RETURN {" +
		"type: type, " +
		"efficiency: ROUND((new_work + refactor)*100/loc)" +
		"}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var aggregations []*models.LanguageEfficiency
	for {
		var item *models.LanguageEfficiency
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		aggregations = append(aggregations, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetLanguagesEfficiency AQL timer [%s]", time.Since(start))

	return aggregations, nil
}

func (dao *CommitDAO) GetCommitsCodeInteraction(rs app.RequestScope, bindings map[string]interface{}) ([]*models.CodeInteraction, error) {
	start := time.Now().UTC()
	languageLinesOfCodeAggregations := dao.buildLanguageLinesOfCodeAggregations(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() + "" +
		"FOR v, e, p IN 1 OUTBOUND c._id blame " +
		"FILTER v != null " +
		"AND c.contributor_id != v.contributor_id " +
		"AND v.active == true " +
		"FOR l IN e.languages[*] " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT from_contributor_id = c.contributor_id, to_contributor_id = v.contributor_id AGGREGATE " +
		languageLinesOfCodeAggregations +
		"RETURN {contributor_from: {_key: from_contributor_id}, contributor_to: {_key: to_contributor_id}, lines_of_code: (additions + modifications + deletions)}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.CodeInteraction
	for {
		var item *models.CodeInteraction
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCommitsCodeInteraction AQL timer [%s]", time.Since(start))

	return result, nil
}

// GetContributorLanguageLinesOfCode returns an aggregation of LOC per language per contributor
func (dao *CommitDAO) GetContributorLanguageLinesOfCode(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorLanguageLinesOfCode, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR l IN c.stats.languages[*] " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT contributor_id = c.contributor_id, language = l.type AGGREGATE " + dao.buildLanguageLinesOfCodeAggregations(bindings) + " " +
		"LET loc = additions + modifications + deletions " +
		"SORT contributor_id ASC, loc DESC " +
		"RETURN {contributor_id: contributor_id, language: language, loc: loc}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.ContributorLanguageLinesOfCode
	for {
		var item *models.ContributorLanguageLinesOfCode
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorLanguageLinesOfCode AQL timer [%s]", time.Since(start))

	return result, nil
}

// GetCommitsContributors returns a list of commit author IDs
func (dao *CommitDAO) GetCommitsContributors(rs app.RequestScope, bindings map[string]interface{}) ([]string, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() + "" +
		"COLLECT contributor_id = c.contributor_id " +
		"RETURN contributor_id"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []string
	for {
		var item string
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCommitsContributors AQL timer [%s]", time.Since(start))

	return result, nil
}

// GetContributorAddedLinesOfCode calculates added loc
func (dao *CommitDAO) GetContributorAddedLinesOfCode(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorLinesOfCode, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR l IN c.stats.languages[*] " +
		"COLLECT contributor_id = c.contributor_id AGGREGATE loc = " + dao.buildLanguageAggregationsSum("additions", bindings) + " " +
		"FILTER loc > 0 " +
		"RETURN {contributor_id, loc}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.ContributorLinesOfCode
	for {
		var item *models.ContributorLinesOfCode

		_, err := cursor.ReadDocument(ctx, &item)
		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorAddedLinesOfCode AQL timer [%s]", time.Since(start))

	return result, nil
}

// GetContributorModifiedLinesOfCode calculates modified loc which were introduced outside of the provided date range
func (dao *CommitDAO) GetContributorModifiedLinesOfCode(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorLinesOfCode, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR v, e, p IN 1 OUTBOUND c._id blame " +
		"FILTER v != null " +
		"AND (v.date < DATE_TIMESTAMP(@start_date) " +
		"OR (v.date >= DATE_TIMESTAMP(@start_date) AND v.date <= DATE_TIMESTAMP(@end_date) AND c.contributor_id != v.contributor_id)) " +
		"AND v.active == true " +
		"FOR l IN e.languages[*] " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT contributor_id = c.contributor_id AGGREGATE loc = " + dao.buildLanguageAggregationsSum("modifications", bindings) + " " +
		"FILTER loc > 0 " +
		"RETURN {contributor_id, loc}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.ContributorLinesOfCode
	for {
		var item *models.ContributorLinesOfCode

		_, err := cursor.ReadDocument(ctx, &item)
		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorModifiedLinesOfCode AQL timer [%s]", time.Since(start))

	return result, nil
}

// GetContributorOverwrittenLinesOfCode calculates loc which were introduced inside of the provided date range and overwritten by others
func (dao *CommitDAO) GetContributorOverwrittenLinesOfCode(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorLinesOfCode, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR v, e, p IN 1 OUTBOUND c._id blame " +
		"FILTER v != null " +
		"AND v.date >= DATE_TIMESTAMP(@start_date) AND v.date <= DATE_TIMESTAMP(@end_date) " +
		"AND c.contributor_id != v.contributor_id " +
		"AND v.active == true " +
		"FOR l IN e.languages[*] " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT contributor_id = v.contributor_id AGGREGATE loc = " + dao.buildLanguageAggregationsSum("deletions", bindings) + " " +
		"FILTER loc > 0 " +
		"RETURN {contributor_id, loc}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.ContributorLinesOfCode
	for {
		var item *models.ContributorLinesOfCode

		_, err := cursor.ReadDocument(ctx, &item)
		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorOverwrittenLinesOfCode AQL timer [%s]", time.Since(start))

	return result, nil
}

// GetContributorDeletedLinesOfCode calculates deleted loc which were introduced inside of the provided date range
func (dao *CommitDAO) GetContributorDeletedLinesOfCode(rs app.RequestScope, bindings map[string]interface{}) ([]*models.ContributorLinesOfCode, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR v, e, p IN 1 OUTBOUND c._id blame " +
		"FILTER v != null " +
		"AND v.date >= DATE_TIMESTAMP(@start_date) AND v.date <= DATE_TIMESTAMP(@end_date) " +
		"AND v.active == true " +
		"FOR l IN e.languages[*] " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT contributor_id = v.contributor_id AGGREGATE loc = " + dao.buildLanguageAggregationsSum("deletions", bindings) + " " +
		"FILTER loc > 0 " +
		"RETURN {contributor_id, loc}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.ContributorLinesOfCode
	for {
		var item *models.ContributorLinesOfCode

		_, err := cursor.ReadDocument(ctx, &item)
		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorDeletedLinesOfCode AQL timer [%s]", time.Since(start))

	return result, nil
}

// GetContributorDailyModifiedLinesOfCode calculates deleted and modified loc which were introduced inside of the provided date range
func (dao *CommitDAO) GetContributorDailyModifiedLinesOfCode(rs app.RequestScope, bindings map[string]interface{}) ([]*models.BucketedContributorLinesOfCode, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR v, e, p IN 1 OUTBOUND c._id blame " +
		"FILTER v != null " +
		"AND v.date >= DATE_TIMESTAMP(@start_date) AND v.date <= DATE_TIMESTAMP(@end_date) " +
		"AND v.active == true " +
		"FOR l IN e.languages[*] " +
		dao.buildLanguagesFilter(bindings) +
		"COLLECT contributor_id = v.contributor_id, " +
		"date_from = c." + dao.getGroupByField(rs, "year_month_day") + ", " +
		"date_to = v." + dao.getGroupByField(rs, "year_month_day") + " AGGREGATE " +
		"deletions = " + dao.buildLanguageAggregationsSum("deletions", bindings) + ", " +
		"modifications = " + dao.buildLanguageAggregationsSum("modifications", bindings) + " " +
		"FILTER (deletions + modifications) > 0 " +
		"AND date_from != date_to " +
		"RETURN {contributor_id: contributor_id, date_from: date_from, date_to: date_to, loc: (deletions + modifications)}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.BucketedContributorLinesOfCode
	for {
		var item *models.BucketedContributorLinesOfCode

		_, err := cursor.ReadDocument(ctx, &item)
		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetContributorDailyModifiedLinesOfCode AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetFileActivityAggregationStats(rs app.RequestScope, bindings map[string]interface{}) ([]*models.FileActivity, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR fl IN c.files[*] " +
		dao.buildFileLanguagesFilter(bindings) +
		"COLLECT file = {_key: fl._key, name: fl.name, path: fl.path, type: fl.type} INTO commits = c.stats " +
		"LET languages = FLATTEN(commits[*].languages) " +
		workTypesVars +
		"RETURN MERGE(file, {" +
		"count: LENGTH(commits), " +
		"work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}" +
		"})"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var filesActivity []*models.FileActivity
	for {
		var item *models.FileActivity
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		filesActivity = append(filesActivity, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetFileActivityAggregationStats AQL timer [%s]", time.Since(start))

	return filesActivity, nil
}

func (dao *CommitDAO) GetCountOfFileCommits(rs app.RequestScope, bindings map[string]interface{}) ([]*models.FileCommitsCount, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR r IN repository FILTER r._key == c.repository_id " +
		"FILTER c.files != null " +
		"FOR fl IN c.files " +
		dao.buildFileFilter(bindings) +
		dao.buildFileLanguagesFilter(bindings) +
		"COLLECT file = {_key: fl._key, name: fl.name, path: fl.path, repository: {_key: r._key, name: r.name, url: r.url}} WITH COUNT INTO count " +
		"RETURN {_key: file._key, name: file.name, path: file.path, repository: file.repository, count: count}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.FileCommitsCount
	for {
		var item *models.FileCommitsCount
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCountOfFileCommits AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetCountOfFileContributors(rs app.RequestScope, bindings map[string]interface{}) ([]*models.FileContributorsCount, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.files != null " +
		"FOR fl IN c.files " +
		dao.buildFileFilter(bindings) +
		dao.buildFileLanguagesFilter(bindings) +
		"COLLECT file = fl._key INTO contributors = c.contributor_id " +
		"RETURN {_key: file, count: LENGTH(UNIQUE(contributors))}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.FileContributorsCount
	for {
		var item *models.FileContributorsCount
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCountOfFileContributors AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetFileWorkTypes(rs app.RequestScope, bindings map[string]interface{}) ([]*models.FileWorkTypes, error) {
	start := time.Now().UTC()

	fileWorkTypesAggregations := dao.buildFileWorkTypesAggregations(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.files != null " +
		"FOR fl IN c.files " +
		dao.buildFileFilter(bindings) +
		dao.buildFileLanguagesFilter(bindings) +
		"COLLECT file = fl._key AGGREGATE " + fileWorkTypesAggregations +
		"RETURN {_key: file, work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.FileWorkTypes
	for {
		var item *models.FileWorkTypes
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetFileWorkTypes AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetFileLinesOfCode(rs app.RequestScope, bindings map[string]interface{}) ([]*models.FileLinesOfCode, error) {
	start := time.Now().UTC()

	linesOfCodeSum := dao.buildLOCSum(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FILTER c.files != null " +
		"SORT c.date ASC " +
		"FOR fl IN c.files " +
		dao.buildFileFilter(bindings) +
		dao.buildFileLanguagesFilter(bindings) +
		"FILTER fl.loc != null " +
		"COLLECT file = fl._key INTO locs = fl.loc " +
		"LET loc = locs[LENGTH(locs)-1] " +
		"RETURN {_key: file, loc: " + linesOfCodeSum + "}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.FileLinesOfCode
	for {
		var item *models.FileLinesOfCode
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetFileLinesOfCode AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetDetails(rs app.RequestScope, bindings map[string]interface{}) (*models.CommitDetails, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)
	linesOfCodeVar := dao.buildLinesOfCodeVars(bindings)

	delete(bindings, "code_style")

	query := "FOR c IN " + CommitCollection + " " +
		"FILTER c._key == @id AND c.active == true " +
		"FOR r IN repository FILTER r._key == c.repository_id " +
		"LET languages = c.stats.languages " +
		"LET pr = (FOR p IN 1 OUTBOUND c._id pull_request_commit " +
		"RETURN {_key: p._key, title: p.title, html_url: p.html_url, number: p.number, body: p.body})[0] " +
		workTypesVars +
		linesOfCodeVar +
		"RETURN {" +
		"_key: c._key, hash: c.hash, date: c.date, message: c.message, " +
		"work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}, " +
		"loc: loc, " +
		"html_url: c.html_url, files: " + dao.filesLengthStr(bindings) + ", pull_request: pr," +
		"repository: {_key: r._key, name: r.name, url: r.url}}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var commitDetails *models.CommitDetails
	for {
		_, err := cursor.ReadDocument(ctx, &commitDetails)

		if driver.IsNoMoreDocuments(err) {
			break
		}
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetDetails AQL timer [%s]", time.Since(start))

	return commitDetails, nil
}

func (dao *CommitDAO) GetCommits(rs app.RequestScope, bindings map[string]interface{}) ([]*models.Commit, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR r IN repository FILTER r._key == c.repository_id " +
		"FOR ctr IN contributor FILTER ctr._key == c.contributor_id " +
		"RETURN {_key: c._key, hash: c.hash, html_url: c.html_url, message: c.message, date: c.date, " +
		"contributor: {_key: ctr._key, first_name: ctr.first_name, last_name: ctr.last_name}, " +
		"repository: {_key: r._key, name: r.name, url: r.url}, " +
		"work_types: {" +
		"new_work: c.stats.work_types.new_work.code + c.stats.work_types.new_work.comments + c.stats.work_types.new_work.blank, " +
		"refactor: c.stats.work_types.refactor.code + c.stats.work_types.refactor.comments + c.stats.work_types.refactor.blank, " +
		"self_churn: c.stats.work_types.self_churn.code + c.stats.work_types.self_churn.comments + c.stats.work_types.self_churn.blank, " +
		"others_churn: c.stats.work_types.others_churn.code + c.stats.work_types.others_churn.comments + c.stats.work_types.others_churn.blank" +
		"}}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.Commit
	for {
		var item *models.Commit
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCommits AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetCommitsDetails(rs app.RequestScope, bindings map[string]interface{}) (*models.CommitsDetails, error) {
	start := time.Now().UTC()

	workTypesVars := dao.buildWorkTypesVars(bindings)
	linesOfCodeVar := dao.buildLinesOfCodeVars(bindings)

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "LET commits = (" +
		"FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"RETURN c.stats.languages) " +
		"LET languages = FLATTEN(commits[*]) " +
		workTypesVars +
		linesOfCodeVar +
		"RETURN {" +
		"commits: LENGTH(commits), " +
		"work_types: {new_work: new_work, refactor: refactor, self_churn: self_churn, others_churn: others_churn}, " +
		"loc: loc}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var commitsDetails *models.CommitsDetails
	for {
		_, err := cursor.ReadDocument(ctx, &commitsDetails)

		if driver.IsNoMoreDocuments(err) {
			break
		}
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCommitsDetails AQL timer [%s]", time.Since(start))

	return commitsDetails, nil
}

func (dao *CommitDAO) GetRecentCommitsActivity(rs app.RequestScope, bindings map[string]interface{}) ([]*models.Activity, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"FOR r IN repository FILTER r._key == c.repository_id " +
		"SORT c.date DESC " +
		"LIMIT @limit " +
		"RETURN {hash: c.hash, date: c.date, html_url: c.html_url, title: c.message, " +
		"repository: {_key: r._key, name: r.name, url: r.url}, " +
		"work_types: {" +
		"new_work: c.stats.work_types.new_work.code + c.stats.work_types.new_work.comments + c.stats.work_types.new_work.blank, " +
		"refactor: c.stats.work_types.refactor.code + c.stats.work_types.refactor.comments + c.stats.work_types.refactor.blank, " +
		"self_churn: c.stats.work_types.self_churn.code + c.stats.work_types.self_churn.comments + c.stats.work_types.self_churn.blank, " +
		"others_churn: c.stats.work_types.others_churn.code + c.stats.work_types.others_churn.comments + c.stats.work_types.others_churn.blank" +
		"}}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.Activity
	for {
		var item *models.Activity
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetRecentCommitsActivity AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetCommitsSummary(rs app.RequestScope, bindings map[string]interface{}) (*models.CommitsSummary, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "LET commits = (FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"RETURN {new_work: c.stats.work_types.new_work, refactor:c.stats.work_types.refactor, self_churn: c.stats.work_types.self_churn, others_churn: c.stats.work_types.others_churn, total: c.stats.loc.total, risk: c.risk, impact: c.impact})" +
		"RETURN {" +
		"count: LENGTH(commits), " +
		"new_work: SUM(commits[*].new_work.code) + SUM(commits[*].new_work.comments) + SUM(commits[*].new_work.blank), " +
		"refactor: SUM(commits[*].refactor.code) + SUM(commits[*].refactor.comments) + SUM(commits[*].refactor.blank), " +
		"self_churn: SUM(commits[*].self_churn.code) + SUM(commits[*].self_churn.comments) + SUM(commits[*].self_churn.blank), " +
		"others_churn: SUM(commits[*].others_churn.code) + SUM(commits[*].others_churn.comments) + SUM(commits[*].others_churn.blank), " +
		"risk: SUM(commits[*].risk), " +
		"impact: SUM(commits[*].impact)}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var commitsSummary *models.CommitsSummary
	for {
		_, err := cursor.ReadDocument(ctx, &commitsSummary)
		if err != nil {
			return nil, err
		}
		break
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetCommitsSummary AQL timer [%s]", time.Since(start))

	return commitsSummary, nil
}

func (dao *CommitDAO) GetFileExtensions(rs app.RequestScope, bindings map[string]interface{}) ([]string, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"LET files = LENGTH(c.files) > 0 ? c.files : [] " +
		"FOR f IN files COLLECT extensions = f.type " +
		"RETURN extensions"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var fileExtensions []string
	for {
		var fileExtension string
		_, err := cursor.ReadDocument(ctx, &fileExtension)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		fileExtensions = append(fileExtensions, fileExtension)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetFileExtensions AQL timer [%s]", time.Since(start))

	return fileExtensions, nil
}

func (dao *CommitDAO) GetLeaderBoardThroughput(rs app.RequestScope, bindings map[string]interface{}) ([]*models.LeaderBoardMetric, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	sortDirection := dao.getSortDirection(&bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT contributor_id = c.contributor_id AGGREGATE " +
		"new_work = SUM(c.stats.work_types.new_work.code + c.stats.work_types.new_work.comments + c.stats.work_types.new_work.blank), " +
		"refactor = SUM(c.stats.work_types.refactor.code + c.stats.work_types.refactor.comments + c.stats.work_types.refactor.blank) " +
		"LET metric = new_work + refactor " +
		"SORT metric " + sortDirection + " " +
		"LIMIT @offset, @limit " +
		"RETURN {contributor: {_key: contributor_id}, metric: metric}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.LeaderBoardMetric
	for {
		var item *models.LeaderBoardMetric
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetLeaderBoardThroughput AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetLeaderBoardEfficiency(rs app.RequestScope, bindings map[string]interface{}) ([]*models.LeaderBoardMetric, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	sortDirection := dao.getSortDirection(&bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT contributor_id = c.contributor_id AGGREGATE " +
		"new_work = SUM(c.stats.work_types.new_work.code + c.stats.work_types.new_work.comments + c.stats.work_types.new_work.blank), " +
		"refactor = SUM(c.stats.work_types.refactor.code + c.stats.work_types.refactor.comments + c.stats.work_types.refactor.blank), " +
		"self_churn = SUM(c.stats.work_types.self_churn.code + c.stats.work_types.self_churn.comments + c.stats.work_types.self_churn.blank), " +
		"others_churn = SUM(c.stats.work_types.others_churn.code + c.stats.work_types.others_churn.comments + c.stats.work_types.others_churn.blank) " +
		"LET metric = ROUND((new_work + refactor)*100/(new_work + refactor + self_churn + others_churn)) " +
		"SORT metric " + sortDirection + " " +
		"LIMIT @offset, @limit " +
		"RETURN {contributor: {_key: contributor_id}, metric: metric}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.LeaderBoardMetric
	for {
		var item *models.LeaderBoardMetric
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetLeaderBoardEfficiency AQL timer [%s]", time.Since(start))

	return result, nil
}

func (dao *CommitDAO) GetLeaderBoardCommits(rs app.RequestScope, bindings map[string]interface{}) ([]*models.LeaderBoardMetric, error) {
	start := time.Now().UTC()

	queryBuilder := newCommitQueryBuilder()
	queryBuilder.buildFilters(bindings)

	sortDirection := dao.getSortDirection(&bindings)

	query := "FOR c IN " + CommitCollection +
		queryBuilder.getFiltersString() +
		"COLLECT contributor_id = c.contributor_id WITH COUNT INTO metric " +
		"SORT metric " + sortDirection + " " +
		"LIMIT @offset, @limit " +
		"RETURN {contributor: {_key: contributor_id}, metric: metric}"

	db, err := rs.Graph(rs.DatabaseName())
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cursor, err := db.Query(ctx, query, bindings)
	if err != nil {
		rs.GetLogger().Error(err)
		return nil, err
	}
	defer cursor.Close()

	var result []*models.LeaderBoardMetric
	for {
		var item *models.LeaderBoardMetric
		_, err := cursor.ReadDocument(ctx, &item)

		if driver.IsNoMoreDocuments(err) {
			break
		}
		result = append(result, item)
	}

	rs.GetLogger().WithFields(logrus.Fields{
		"aql":      query,
		"bindings": bindings,
	}).Infof("GetLeaderBoardCommits AQL timer [%s]", time.Since(start))

	return result, nil
}

// getGroupBy parses the group_by value and maps it with a proper query field to group commits by
func (dao *CommitDAO) getGroupBy(rs app.RequestScope, bindings *map[string]interface{}, name string) string {
	localBindings := *bindings

	v, _ := localBindings[name].(string)
	groupBy := dao.getGroupByField(rs, v)

	delete(localBindings, name)
	bindings = &localBindings

	return groupBy
}

// getGroupByField returns path to the requested date group variable
func (dao *CommitDAO) getGroupByField(rs app.RequestScope, name string) string {
	offset := strconv.Itoa(rs.User().GetOffset() / 3600)

	switch name {
	case "year":
		return "date_groups[" + offset + "].year"
	case "month":
		return "date_groups[" + offset + "].month"
	case "day_of_week":
		return "date_groups[" + offset + "].day_of_week"
	case "hour":
		return "date_groups[" + offset + "].hour"
	case "year_month":
		return "date_groups[" + offset + "].year_month"
	case "year_week":
		return "date_groups[" + offset + "].year_week"
	case "year_month_day":
		return "date_groups[" + offset + "].year_month_day"
	default:
		return "date"
	}
}

// getSortDirection parses the sort_direction value, sets the default value if not found
func (dao *CommitDAO) getSortDirection(bindings *map[string]interface{}) string {
	localBindings := *bindings

	if _, ok := localBindings["sort_direction"].(string); ok == false {
		localBindings["sort_direction"] = "DESC"
	}

	sortDirection := localBindings["sort_direction"].(string)
	delete(localBindings, "sort_direction")

	bindings = &localBindings

	return sortDirection
}

// getWorkTypeSort parses the sort value
func (dao *CommitDAO) getWorkTypeSort(bindings *map[string]interface{}) string {
	localBindings := *bindings

	sort, _ := localBindings["sort"].(string)
	delete(localBindings, "sort")

	bindings = &localBindings

	return sort
}

// getMostActiveFilesSort parses the sort value
func (dao *CommitDAO) getMostActiveFilesSort(bindings *map[string]interface{}) string {
	localBindings := *bindings

	sort, _ := localBindings["sort"].(string)
	delete(localBindings, "sort")

	bindings = &localBindings

	return sort
}

// Dynamically Compose return work_types, language specific, filters
func (dao *CommitDAO) buildLanguageWorkTypeSum(workType string, codeStyles []string) string {
	var codeStylesSum []string
	for _, codeStyle := range codeStyles {
		codeStylesSum = append(codeStylesSum, "SUM(languages[* FILTER CURRENT.type IN @languages RETURN CURRENT.work_types."+workType+"."+codeStyle+"])")
	}

	return "SUM([" + strings.Join(codeStylesSum, ",") + "])"
}

// Dynamically Compose return work_types filters
func (dao *CommitDAO) buildWorkTypeSum(workType string, codeStyles []string) string {
	var codeStylesSum []string
	for _, codeStyle := range codeStyles {
		codeStylesSum = append(codeStylesSum, "SUM(languages[*].work_types."+workType+"."+codeStyle+")")
	}

	return "SUM([" + strings.Join(codeStylesSum, ",") + "])"
}

// buildLanguageLinesOfCodeSum composes return loc, language specific, filters
func (dao *CommitDAO) buildLanguageLinesOfCodeSum(lineOfCodeType string, codeStyles []string) string {
	var codeStylesSum []string
	for _, codeStyle := range codeStyles {
		codeStylesSum = append(codeStylesSum, "SUM(languages[* FILTER CURRENT.type IN @languages RETURN CURRENT.loc."+lineOfCodeType+"."+codeStyle+"])")
	}

	return "SUM([" + strings.Join(codeStylesSum, ",") + "])"
}

// Dynamically Compose return work_types filters
func (dao *CommitDAO) buildLinesOfCodeSum(lineOfCodeType string, codeStyles []string) string {
	var codeStylesSum []string
	for _, codeStyle := range codeStyles {
		codeStylesSum = append(codeStylesSum, "SUM(languages[*].loc."+lineOfCodeType+"."+codeStyle+")")
	}

	return "SUM([" + strings.Join(codeStylesSum, ",") + "])"
}

// buildWorkTypesVars builds a work_types query string as variables
// Query contains language and code_style filters constructed based on the bindings data
func (dao *CommitDAO) buildWorkTypesVars(bindings map[string]interface{}) string {
	// Either do all code styles, or use what the filter says
	codeStyles := []string{"code", "comments", "blank"}
	if _, ok := bindings["code_style"].([]string); ok == true {
		codeStyles = bindings["code_style"].([]string)
	}

	var workTypesVars string
	// Gor for stats.languages.work_types in case of set language filter
	if _, ok := bindings["languages"].([]string); ok == true {
		workTypesVars = "" +
			"LET new_work = " + dao.buildLanguageWorkTypeSum("new_work", codeStyles) + " " +
			"LET refactor = " + dao.buildLanguageWorkTypeSum("refactor", codeStyles) + " " +
			"LET self_churn = " + dao.buildLanguageWorkTypeSum("self_churn", codeStyles) + " " +
			"LET others_churn = " + dao.buildLanguageWorkTypeSum("others_churn", codeStyles) + " "
	} else {
		workTypesVars = "" +
			"LET new_work = " + dao.buildWorkTypeSum("new_work", codeStyles) + " " +
			"LET refactor = " + dao.buildWorkTypeSum("refactor", codeStyles) + " " +
			"LET self_churn = " + dao.buildWorkTypeSum("self_churn", codeStyles) + " " +
			"LET others_churn = " + dao.buildWorkTypeSum("others_churn", codeStyles) + " "
	}

	return workTypesVars
}

// buildLinesOfCodeVars builds an loc query string as a variable
// Query contains language and code_style filters constructed based on the bindings data
func (dao *CommitDAO) buildLinesOfCodeVars(bindings map[string]interface{}) string {
	// Either do all code styles, or use what the filter says
	codeStyles := []string{"code", "comments", "blank"}
	if _, ok := bindings["code_style"].([]string); ok == true {
		codeStyles = bindings["code_style"].([]string)
	}

	var linesOfCodeVar string
	// Gor for stats.languages.loc in case of set language filter
	if _, ok := bindings["languages"].([]string); ok == true {
		linesOfCodeVar = "LET loc = {" +
			"additions: " + dao.buildLanguageLinesOfCodeSum("additions", codeStyles) + ", " +
			"modifications: " + dao.buildLanguageLinesOfCodeSum("modifications", codeStyles) + ", " +
			"deletions: " + dao.buildLanguageLinesOfCodeSum("deletions", codeStyles) + ", " +
			"total: " + dao.buildLanguageLinesOfCodeSum("total", codeStyles) + "} "
	} else {
		linesOfCodeVar = "LET loc = {" +
			"additions: " + dao.buildLinesOfCodeSum("additions", codeStyles) + ", " +
			"modifications: " + dao.buildLinesOfCodeSum("modifications", codeStyles) + ", " +
			"deletions: " + dao.buildLinesOfCodeSum("deletions", codeStyles) + ", " +
			"total: " + dao.buildLinesOfCodeSum("total", codeStyles) + "} "
	}

	return linesOfCodeVar
}

func (dao *CommitDAO) buildFileLanguagesFilter(bindings map[string]interface{}) string {
	if _, ok := bindings["languages"].([]string); ok == true {
		return "FILTER fl.type IN @languages "
	}

	return ""
}

func (dao *CommitDAO) buildLanguagesFilter(bindings map[string]interface{}) string {
	if _, ok := bindings["languages"].([]string); ok == true {
		return "FILTER l.type IN @languages "
	}

	return ""
}

func (dao *CommitDAO) buildFileFilter(bindings map[string]interface{}) string {
	if _, ok := bindings["file_id"].(string); ok == true {
		return "FILTER fl._key == @file_id "
	}

	return ""
}

// buildFileWorkTypesVars builds a work_types query string as variables
// Query contains code_style filters constructed based on the bindings data
func (dao *CommitDAO) buildFileWorkTypesVars(bindings map[string]interface{}) string {
	// Either do all code styles, or use what the filter says
	codeStyles := []string{"code", "comments", "blank"}
	if _, ok := bindings["code_style"].([]string); ok == true {
		codeStyles = bindings["code_style"].([]string)
	}

	workTypesVars := "" +
		"LET new_work = " + dao.buildFileWorkTypeSum("new_work", codeStyles) + " " +
		"LET refactor = " + dao.buildFileWorkTypeSum("refactor", codeStyles) + " " +
		"LET self_churn = " + dao.buildFileWorkTypeSum("self_churn", codeStyles) + " " +
		"LET others_churn = " + dao.buildFileWorkTypeSum("others_churn", codeStyles) + " "

	return workTypesVars
}

// buildFileWorkTypesAggregations builds a work_types query string to be used as an aggregation string
// Query contains code_style filters constructed based on the bindings data
func (dao *CommitDAO) buildFileWorkTypesAggregations(bindings map[string]interface{}) string {
	// Either do all code styles, or use what the filter says
	codeStyles := []string{"code", "comments", "blank"}
	if _, ok := bindings["code_style"].([]string); ok == true {
		codeStyles = bindings["code_style"].([]string)
	}

	workTypesAggregation := "" +
		"new_work = SUM(" + dao.buildFileWorkTypeAggregationSum("new_work", codeStyles) + "), " +
		"refactor = SUM(" + dao.buildFileWorkTypeAggregationSum("refactor", codeStyles) + "), " +
		"self_churn = SUM(" + dao.buildFileWorkTypeAggregationSum("self_churn", codeStyles) + "), " +
		"others_churn = SUM(" + dao.buildFileWorkTypeAggregationSum("others_churn", codeStyles) + ") "

	return workTypesAggregation
}

// Dynamically Compose return work_types filters
func (dao *CommitDAO) buildFileWorkTypeSum(workType string, codeStyles []string) string {
	var codeStylesSum []string
	for _, codeStyle := range codeStyles {
		codeStylesSum = append(codeStylesSum, "SUM(work_types[*]."+workType+"."+codeStyle+")")
	}

	return "SUM([" + strings.Join(codeStylesSum, ",") + "])"
}

// Dynamically Compose return work_types filters
func (dao *CommitDAO) buildFileWorkTypeAggregationSum(workType string, codeStyles []string) string {
	var codeStylesSum []string
	for _, codeStyle := range codeStyles {
		codeStylesSum = append(codeStylesSum, "fl.stats.work_types."+workType+"."+codeStyle)
	}

	return "SUM([" + strings.Join(codeStylesSum, ",") + "])"
}

func (dao *CommitDAO) buildLOCSum(bindings map[string]interface{}) string {
	var codeStylesSum []string

	codeStyles := []string{"code", "comments", "blank"}
	if _, ok := bindings["code_style"].([]string); ok == true {
		codeStyles = bindings["code_style"].([]string)
	}

	for _, codeStyle := range codeStyles {
		codeStylesSum = append(codeStylesSum, "loc."+codeStyle+"")
	}

	return "SUM([" + strings.Join(codeStylesSum, ",") + "])"
}

// buildLanguageLinesOfCodeAggregations builds an loc query string to be used as an aggregation string
// Query contains code_style filters constructed based on the bindings data
func (dao *CommitDAO) buildLanguageLinesOfCodeAggregations(bindings map[string]interface{}) string {
	// Either do all code styles, or use what the filter says
	codeStyles := []string{"code", "comments", "blank"}
	if _, ok := bindings["code_style"].([]string); ok == true {
		codeStyles = bindings["code_style"].([]string)
	}

	workTypesAggregation := "" +
		"additions = SUM(" + dao.buildLanguageLinesOfCodeAggregationsSum("additions", codeStyles) + "), " +
		"modifications = SUM(" + dao.buildLanguageLinesOfCodeAggregationsSum("modifications", codeStyles) + "), " +
		"deletions = SUM(" + dao.buildLanguageLinesOfCodeAggregationsSum("deletions", codeStyles) + ") "

	return workTypesAggregation
}

// buildLanguageAdditionsAggregationsSum builds an loc query string to be used as an aggregation string
func (dao *CommitDAO) buildLanguageAggregationsSum(name string, bindings map[string]interface{}) string {
	// Either do all code styles, or use what the filter says
	codeStyles := []string{"code", "comments", "blank"}
	if _, ok := bindings["code_style"].([]string); ok == true {
		codeStyles = bindings["code_style"].([]string)
	}

	return "SUM(" + dao.buildLanguageLinesOfCodeAggregationsSum(name, codeStyles) + ")"
}

// Dynamically Compose return work_types filters
func (dao *CommitDAO) buildLanguageLinesOfCodeAggregationsSum(linesOfCodeType string, codeStyles []string) string {
	var codeStylesSum []string
	for _, codeStyle := range codeStyles {
		codeStylesSum = append(codeStylesSum, "l.loc."+linesOfCodeType+"."+codeStyle)
	}

	return "SUM([" + strings.Join(codeStylesSum, ",") + "])"
}

func (dao *CommitDAO) filesLengthStr(bindings map[string]interface{}) string {
	if _, ok := bindings["languages"].([]string); ok == true {
		return "LENGTH(c.files[* FILTER CURRENT.type IN @languages RETURN CURRENT])"
	} else {
		return "LENGTH(c.files)"
	}
}
