#pragma once
#include <cstdlib>
#include <iostream>
#include <memory>
#include <alibabacloud/core/AlibabaCloud.h>
#include <alibabacloud/core/CommonRequest.h>
#include <alibabacloud/core/CommonClient.h>
#include <alibabacloud/core/CommonResponse.h>
#include "logger.hpp"

namespace hmy{
class DMSClient
{
public:
    DMSClient(const std::string& access_key_id, const std::string& access_key_secret)
    :_access_key_id(access_key_id), _access_key_secret(access_key_secret)
    {
        AlibabaCloud::InitializeSdk();
        AlibabaCloud::ClientConfiguration configuration( "cn-shenzhen" );
        configuration.setConnectTimeout(1500);
        configuration.setReadTimeout(4000);
        AlibabaCloud::Credentials credential(_access_key_id, _access_key_secret);
        _client = std::make_unique<AlibabaCloud::CommonClient>(credential, configuration);
    }
    ~DMSClient()
    {
        AlibabaCloud::ShutdownSdk();
    }

    bool Send(const std::string& phone, const std::string& code)
    {
        AlibabaCloud::CommonRequest request(AlibabaCloud::CommonRequest::RequestPattern::RpcPattern);
        request.setHttpMethod(AlibabaCloud::HttpRequest::Method::Post);
        request.setDomain("dysmsapi.aliyuncs.com");
        request.setVersion("2017-05-25");
        request.setQueryParameter("Action", "SendSms");
        request.setQueryParameter("SignName", "Chat聊天");
        request.setQueryParameter("TemplateCode", "SMS_478945376");
        request.setQueryParameter("PhoneNumbers", phone);
        std::string verifyCode = "{\"code\":\"" + code + "\"}";
        request.setQueryParameter("TemplateParam", verifyCode);

        auto response = _client->commonResponse(request);
        if (!response.isSuccess()) {
            LOG_ERROR("短信验证码请求失败: {}", response.error().errorMessage().c_str());
            return false;
        }

        LOG_ERROR("短信验证码请求成功: {}", response.result().payload().c_str());
        return true;
    } 
private:
    std::string _access_key_id;
    std::string _access_key_secret;
    std::unique_ptr<AlibabaCloud::CommonClient> _client;
};
}