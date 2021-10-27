using System;
using System.IO;
using System.Collections.Generic;

using Newtonsoft.Json;

namespace SPH.PLAS.ESA
{
    public class NewtonJsonWrapper
    {
        #region Constructors.
        public NewtonJsonWrapper()
        {
            _serializer = new JsonSerializer();

            _serializer.DateFormatHandling = DateFormatHandling.MicrosoftDateFormat;
            _serializer.NullValueHandling = NullValueHandling.Ignore;
            _serializer.Formatting = Formatting.Indented;
        }
        #endregion

        #region Public Methods.
        public void Write<T>(T obj, string fullFilePath)
        {
            using (StreamWriter sw = new StreamWriter(fullFilePath))
            using (JsonWriter writer = new JsonTextWriter(sw))
            {
                _serializer.Serialize(writer, obj);
            }
        }

        public T Read<T>(string fullFilePath)
        {
            using (StreamReader sr = new StreamReader(fullFilePath, System.Text.Encoding.Default))
            {
                return (T)_serializer.Deserialize(sr, typeof(T));
            }
        }
        public byte[] GetBytes<T>(T obj)
        {
            byte[] result = default;

            using (var stream = new MemoryStream())
            using (StreamWriter writer = new StreamWriter(stream))
            using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
            {
                _serializer.Serialize(jsonWriter, obj);
                jsonWriter.Flush();
                result = stream.ToArray();
            }

            return result;
        }

        public T GetObject<T>(byte[] bytes)
        {
            T result = default;

            using (var stream = new MemoryStream(bytes))
            {
                using (StreamReader reader = new StreamReader(stream))
                using (JsonTextReader jsonReader = new JsonTextReader(reader))
                {
                    result = _serializer.Deserialize<T>(jsonReader);
                }
            }

            return result;
        }
        #endregion

        #region Private Members.
        private JsonSerializer _serializer { get; set; }
        #endregion
    }
}