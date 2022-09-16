package main

import (
	"os"
	"errors"
	"context"
	"time"
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/prometheus/common/model"
)

type KinesisFirehoseHttpEndpointRequest struct {
	RequestID string `json:"requestId"`
	Timestamp int64  `json:"timestamp"`
	Records   []struct {
		Data string `json:"data"`
	} `json:"records"`
}

type KinesisFirehoseHttpEndpointResponse struct {
	RequestID string `json:"requestId"`
	Timestamp int64  `json:"timestamp"`
	ErrorMessage string `json:"errorMessage"`
}

func parseKinesisFirehoseHttpEndpointResponseToPointerString(response KinesisFirehoseHttpEndpointResponse)  *string {
	kinesisFirehoseHttpEndpointResponse, err := json.Marshal(response)

	if err != nil {
		return nil
	}

	kinesisFirehoseHttpEndpointResponseString := string(kinesisFirehoseHttpEndpointResponse)
	return &kinesisFirehoseHttpEndpointResponseString
}

func parseKinesisFirehoseHttpEndpointRequest(ctx context.Context, b *batch, ev *events.APIGatewayV2HTTPRequest) (response KinesisFirehoseHttpEndpointResponse, err error) {

	body := KinesisFirehoseHttpEndpointRequest{}
    json.Unmarshal([]byte(ev.Body), &body)
	response = KinesisFirehoseHttpEndpointResponse{
		body.RequestID,body.Timestamp,"",
	}

	if (os.Getenv("FIREHOSE_ACCESS_KEY") != "" && ev.Headers["x-amz-firehose-access-key"] != os.Getenv("FIREHOSE_ACCESS_KEY")) {
		err := errors.New("x-amz-firehose-access-key unauthorized")
		response.ErrorMessage = err.Error()
		return response,err
	}

	timestamp := time.UnixMilli(body.Timestamp)

	for _, record := range body.Records {
		data, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			response.ErrorMessage = err.Error()
			return response,err
		}

		labels := model.LabelSet{
			model.LabelName("__aws_log_type"): model.LabelValue("kinesis_delivery_stream"),
			model.LabelName("__aws_kinesis_firehose_source_arn"): model.LabelValue(ev.Headers["x-amz-firehose-source-arn"]),
			model.LabelName("__aws_kinesis_firehose_request_id"): model.LabelValue(body.RequestID),
		}

		labels = applyExtraLabels(labels)

		b.add(ctx, entry{labels, logproto.Entry{
			Line:      string(data),
			Timestamp: timestamp,
		}})
	}

	return response,nil
}

func processKinesisFirehoseHttpEndpointRequest(ctx context.Context, ev *events.APIGatewayV2HTTPRequest) (response *string, err error) {
	batch, _ := newBatch(ctx)
	kinesisFirehoseHttpEndpointResponse,err := parseKinesisFirehoseHttpEndpointRequest(ctx, batch, ev)
	if err != nil {
		kinesisFirehoseHttpEndpointResponse.ErrorMessage = err.Error()
		return parseKinesisFirehoseHttpEndpointResponseToPointerString(kinesisFirehoseHttpEndpointResponse),err
	}

	err = sendToPromtail(ctx, batch)
	if err != nil {
		kinesisFirehoseHttpEndpointResponse.ErrorMessage = err.Error()
		return parseKinesisFirehoseHttpEndpointResponseToPointerString(kinesisFirehoseHttpEndpointResponse),err
	}

	return parseKinesisFirehoseHttpEndpointResponseToPointerString(kinesisFirehoseHttpEndpointResponse),nil
}
