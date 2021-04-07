# JOSE decryption

This SMT allows you to decrypt byte messages that have been encrypted using JOSE.

## Usage

```
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter

transforms=jose
transforms.jose.type=com.birdie.kafka.connect.smt.Jose$DecryptValue
transforms.jose.key="encryption-key"
```

## Properties

|Name|Description|Type|Default|
|---|---|---|---|
|`keys`| Comma separated list of base64 encoded JWKs | String | ø |
|`skip-on-error`| If `true`, skips messages it cannot decrypt (i.e. keeps the original payload) | String | `false` |
