from marshmallow import Schema, fields


class TopologyJSONSchema(Schema):
    """Schema for Topology."""

    username = fields.String(required=True)
    password = fields.String(required=True)
    endpoint = fields.String(required=True)
