package app

import (
	"encoding/json"

	"github.com/carlescere/scheduler"
	"github.com/gitalytics/alerts/pkg/database"
	"github.com/gitalytics/alerts/pkg/integration"
	Interfaces "github.com/gitalytics/alerts/pkg/interface/scope"
	"github.com/gitalytics/alerts/pkg/processor"
	"github.com/gitalytics/alerts/pkg/scope"
	"github.com/gitalytics/messenger"
	"github.com/gitalytics/messenger/payload"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// scopePool is a pool of database scopes
var scopePool = map[string]Interfaces.Scope{}

// ConnectToQueue collects all of the available hashes from a repository
// and adds them to the queue for processing
func ConnectToQueue(db *database.DBModels, log *logrus.Entry, queue *messenger.AmqpClient, slack *integration.SlackService) {
	localScope := scope.NewScope(db, log, slack)
	localScope.SetQueue(queue)
	queueName := db.Name + ":alerts"

	if err := localScope.Queue().SubscribeToQueue(queueName, "alerts", onMessageDelivery); err != nil {
		localScope.Log().Fatalf("failed subscribe to the \"%s\" queue: %s", queueName, err)
	}

	scopePool[db.Name] = localScope

	localScope.Log().Info("[*] Waiting for messages")
}

// SetupScheduler initializes scheduler for the database
func SetupScheduler(db *database.DBModels) {
	if _, ok := scopePool[db.Name]; !ok {
		logrus.Errorf("scope not found for db %s", db.Name)
		return
	}

	localScope := scopePool[db.Name]

	// Set up scheduler for the pull request age processor
	scheduler.Every(Conf.ApplicationInterval).Seconds().Run(func() {
		if err := processor.PullRequestAge(localScope); err != nil {
			localScope.Log().Errorf("error processing pull_request_age alerts: %v", err)
		}
	})
}

// onMessageDelivery is the callback that's invoked whenever we get a message
func onMessageDelivery(delivery amqp.Delivery) {
	var queueItem *payload.AlertPayload

	if err := json.Unmarshal([]byte(delivery.Body), &queueItem); err != nil {
		logrus.WithError(err).WithField("message_id", delivery.MessageId).Errorln("could not unmarshal")

		delivery.Nack(false, false)
		return
	}

	if _, ok := scopePool[queueItem.DatabaseID]; !ok {
		logrus.WithField("message_id", delivery.MessageId).Errorf("scope not found for db %s", queueItem.DatabaseID)

		delivery.Nack(false, false)
		return
	}

	sc := setupScope(scopePool[queueItem.DatabaseID], queueItem.RepositoryID)
	sc.SetLog(sc.Log().WithFields(logrus.Fields{
		"type":   queueItem.Type,
		"entity": queueItem.EntityID,
	}))

	switch queueItem.Type {
	case payload.CommitPayloadType:
		if err := checkCommitAlerts(queueItem, sc); err != nil {
			sc.Log().WithField("error", err).Error("error scanning alerts for commit")
			delivery.Nack(false, false)
		}

		sc.Log().Info("scanned commit for alerts")
	case payload.PullRequestPayloadType:
		if err := checkPullRequestAlerts(queueItem, sc); err != nil {
			sc.Log().WithField("error", err).Error("error scanning alerts for pull request")
			delivery.Nack(false, false)
		}

		sc.Log().Info("scanned pull request for alerts")
	case payload.IssuePayloadType:
		if err := checkIssueAlerts(queueItem, sc); err != nil {
			sc.Log().WithField("error", err).Error("error scanning alerts for issue")
			delivery.Nack(false, false)
		}

		sc.Log().Info("scanned issue for alerts")

	default:
		sc.Log().Error("unknown payload type \"%s\"", queueItem.Type)
	}

	// acknowledge the analyzer has completed this a delivery
	if err := delivery.Ack(false); err != nil {
		sc.Log().WithField("error", err).Info("could not acknowledge delivery")
	}
}

// setupScope checks for a repo record that needs processing
func setupScope(sc Interfaces.Scope, repositoryID string) Interfaces.Scope {
	repo, err := sc.DB().Repo.Get(repositoryID)
	if err != nil {
		sc.Log().WithField("error", err).Panic("could not retrieve repository with id: " + repositoryID)
	}

	sc.SetRepo(repo)

	return sc
}

func checkCommitAlerts(payload *payload.AlertPayload, sc Interfaces.Scope) error {
	if err := processor.CommitSize(payload, sc); err != nil {
		sc.Log().WithField("error", err).Error("error processing commit_size alert")
		return err
	}

	if err := processor.ChurnRefactor(payload, sc); err != nil {
		sc.Log().WithField("error", err).Error("error processing churn_refactor alert")
		return err
	}

	if err := processor.Efficiency(payload, sc); err != nil {
		sc.Log().WithField("error", err).Error("error processing efficiency alert")
		return err
	}

	return nil
}

func checkPullRequestAlerts(payload *payload.AlertPayload, sc Interfaces.Scope) error {
	if err := processor.PullRequestComments(payload, sc); err != nil {
		sc.Log().Errorf("error processing pull_request_comments alerts: %v", err)
		return err
	}

	if err := processor.PullRequestCommenters(payload, sc); err != nil {
		sc.Log().Errorf("error processing pull_request_commenters alerts: %v", err)
		return err
	}

	if err := processor.PullRequestSize(payload, sc); err != nil {
		sc.Log().Errorf("error processing pull_request_size alerts: %v", err)
		return err
	}

	return nil
}

func checkIssueAlerts(payload *payload.AlertPayload, sc Interfaces.Scope) error {
	return nil
}

