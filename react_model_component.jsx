// React component example for using the Snowflake ML Model
// This uses the Snowflake SQL API to call the registered model

import React, { useState } from 'react';

const SHIP_OPTIONS = ['Carnival Breeze', 'Carnival Celebration', 'Carnival Horizon', 'Carnival Jubilee', 'Carnival Magic', 'Carnival Panorama', 'Carnival Vista', 'Mardi Gras'];
const GAME_OPTIONS = ['Classic Slots', 'Bonus Slots', 'Progressive Slots', 'Video Poker'];
const TIER_OPTIONS = ['Basic', 'Bronze', 'Gold', 'Platinum', 'Silver'];
const RISK_OPTIONS = ['Conservative', 'High', 'Moderate'];
const INCOME_OPTIONS = ['Under $50K', '$50K-$75K', '$75K-$100K', '$100K-$150K', 'Over $150K'];
const DENOM_MAP = ['$0.01', '$0.05', '$0.25', '$1.00', '$5.00', '$10.00', '$20.00', '$50.00', '$100.00'];

export function DenominationPredictor({ executeQuery }) {
  const [formData, setFormData] = useState({
    ship: 0, month: 6, dayOfWeek: 2, isWeekend: 0,
    game: 0, age: 45, gender: 0, tier: 0,
    cruises: 5, risk: 0, income: 2
  });
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(false);

  const handlePredict = async () => {
    setLoading(true);
    const sql = `
      SELECT MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_DENOMINATION_MODEL, V1)!PREDICT(
        ${formData.ship}, ${formData.month}, ${formData.dayOfWeek}, ${formData.isWeekend},
        ${formData.game}, ${formData.age}, ${formData.gender}, ${formData.tier},
        ${formData.cruises}, ${formData.risk}, ${formData.income}
      ):output_feature_0 AS predicted_class
    `;
    
    try {
      const result = await executeQuery(sql);
      const predClass = result[0].PREDICTED_CLASS;
      setPrediction({ class: predClass, denomination: DENOM_MAP[predClass] });
    } catch (error) {
      console.error('Prediction failed:', error);
    }
    setLoading(false);
  };

  return (
    <div className="p-6 bg-gray-900 rounded-lg">
      <h2 className="text-2xl font-bold text-white mb-4">🎰 Denomination Predictor</h2>
      
      <div className="grid grid-cols-3 gap-4 mb-6">
        <select 
          className="bg-gray-800 text-white p-2 rounded"
          value={formData.ship}
          onChange={(e) => setFormData({...formData, ship: parseInt(e.target.value)})}
        >
          {SHIP_OPTIONS.map((s, i) => <option key={s} value={i}>{s}</option>)}
        </select>
        
        <select
          className="bg-gray-800 text-white p-2 rounded"
          value={formData.game}
          onChange={(e) => setFormData({...formData, game: parseInt(e.target.value)})}
        >
          {GAME_OPTIONS.map((g, i) => <option key={g} value={i}>{g}</option>)}
        </select>
        
        <input
          type="number" min="21" max="85"
          className="bg-gray-800 text-white p-2 rounded"
          value={formData.age}
          onChange={(e) => setFormData({...formData, age: parseInt(e.target.value)})}
          placeholder="Age"
        />
        
        <select
          className="bg-gray-800 text-white p-2 rounded"
          value={formData.tier}
          onChange={(e) => setFormData({...formData, tier: parseInt(e.target.value)})}
        >
          {TIER_OPTIONS.map((t, i) => <option key={t} value={i}>{t}</option>)}
        </select>
        
        <select
          className="bg-gray-800 text-white p-2 rounded"
          value={formData.risk}
          onChange={(e) => setFormData({...formData, risk: parseInt(e.target.value)})}
        >
          {RISK_OPTIONS.map((r, i) => <option key={r} value={i}>{r}</option>)}
        </select>
        
        <select
          className="bg-gray-800 text-white p-2 rounded"
          value={formData.income}
          onChange={(e) => setFormData({...formData, income: parseInt(e.target.value)})}
        >
          {INCOME_OPTIONS.map((inc, i) => <option key={inc} value={i}>{inc}</option>)}
        </select>
      </div>
      
      <button
        onClick={handlePredict}
        disabled={loading}
        className="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-4 rounded"
      >
        {loading ? 'Predicting...' : 'Predict Denomination'}
      </button>
      
      {prediction && (
        <div className="mt-4 p-4 bg-green-900 rounded text-center">
          <p className="text-xl text-white">Recommended: <strong>{prediction.denomination}</strong></p>
        </div>
      )}
    </div>
  );
}

// Example SQL for direct API usage:
// SELECT MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_DENOMINATION_MODEL, V1)!PREDICT(
//   0, 6, 2, 0, 0, 45, 0, 2, 5, 1, 2
// ):output_feature_0 AS predicted_class;
