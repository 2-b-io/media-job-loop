import config from 'infrastructure/config'
import route53 from 'infrastructure/route-53'

const getHostedZone = async () => {
  const hostedZone = await route53.getHostedZone({
    Id: config.aws.route53.hostedZoneId
  }).promise()

  return hostedZone
}

const createRecordSet = async (name, value) => {
  return await route53.changeResourceRecordSets({
    ChangeBatch: {
      Changes: [ {
        Action: 'CREATE',
        ResourceRecordSet: {
          AliasTarget: {
            DNSName: value,
            EvaluateTargetHealth: false,
            HostedZoneId: 'Z2FDTNDATAQYW2'
          },
          Name: name,
          Type: 'A'
        }
      } ],
      Comment: `Map CloudFront distribution for ${ name }`
    },
    HostedZoneId: config.aws.route53.hostedZoneId
  }).promise()
}

const removeRecordSet = async (name, value) => {
  return await route53.changeResourceRecordSets({
    ChangeBatch: {
      Changes: [ {
        Action: 'DELETE',
        ResourceRecordSet: {
          AliasTarget: {
            DNSName: value,
            EvaluateTargetHealth: false,
            HostedZoneId: 'Z2FDTNDATAQYW2'
          },
          Name: name,
          Type: 'A'
        }
      } ],
      Comment: `Unmap CloudFront distribution for ${ name }`
    },
    HostedZoneId: config.aws.route53.hostedZoneId
  }).promise()
}

export default {
  getHostedZone,
  createRecordSet,
  removeRecordSet
}
