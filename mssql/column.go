package mssql

var usersTableColumns = []string{
	"old_user_id",
	"open_id",
	"photo",
	"email",
	"first_name",
	"last_name",
	"normalized_name",
	"gender",
	"account_id",
	"date_of_birth",
	"url",
	"login_id",
	"is_deleted",
	"on_created",
	"on_updated",
	"on_deleted",
}

var groupsTableColumns = []string{
	"old_group_id",
	"photo",
	"name",
	"normalized_name",
	"description",
	"sports_type",
	"age",
	"gender",
	"skill_level",
	"url",
	"on_created",
	"on_updated",
	"on_deleted",
	"is_deleted",
	"is_private",
}

var eventsTableColumns = []string{
	"photo",
	"sports_type",
	"title",
	"normalized_title",
	"start",
	"end",
	"time_zone",
	"place_id",
	"place_name",
	"location_details",
	"description",
	"fee",
	"age",
	"gender",
	"skill_level",
	"field_type",
	"has_rsvp_deadline",
	"rsvp_deadline",
	"has_participant_limit",
	"participant_limit",
	"allow_guests",
	"guests",
	"is_recurring",
	"old_event_id",
	"old_group_id",
	"old_host_id",
	"is_deleted",
	"on_created",
	"on_updated",
	"on_deleted",
}

var paymentsTableColumns = []string{
	"id",
	"amount",
	"fee_amount",
	"customer_id",
	"refund_id",
	"card_brand",
	"card_last4_digits",
	"currency",
	"old_event_id",
	"old_user_id",
	"on_created",
}

func getUsersTableColumns() []string {
	return usersTableColumns
}

func getGroupsTableColumns() []string {
	return groupsTableColumns
}

func getEventsTableColumns() []string {
	return eventsTableColumns
}

func getPaymentsTableColumns() []string {
	return paymentsTableColumns
}
